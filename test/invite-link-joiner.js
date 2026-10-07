import test from 'node:test'
import { TypedEmitter } from 'tiny-typed-emitter'
import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import { Transform } from 'streamx'
import { pEvent } from 'p-event'
import pDefer from 'p-defer'

import { InviteLinkJoiner } from '../src/invite/invite-link-joiner.js'
import { makeInviteURL } from '../src/invite/invite-urls.js'
import { MEMBER_ROLE_ID } from '../src/roles.js'
import {
  InviteConnectionError,
  InviteDeniedByInviterError,
  InviteRedeemConnectionClosedError,
  JoinProjectCancelledError,
} from '../src/errors.js'

/** @import { RemoteAuthedNoiseStream } from '../src/discovery/remote-discovery.js' */
/** @import { Invite, InviteApi } from '../src/invite/invite-api.js' */
/** @import { JoinerDiscovery, JoinRequestUpdate } from '../src/invite/invite-link-joiner.js' */
/** @import { Redeem } from '../src/generated/invite-link.js' */

/**
 * @param {Buffer} authenticatedPublicKey
 * @returns {RemoteAuthedNoiseStream}
 */
function mockConnection(authenticatedPublicKey) {
  const connection = /** @type {RemoteAuthedNoiseStream} */ (
    /** @type {unknown} */ (new Transform())
  )
  connection.authenticatedPublicKey = authenticatedPublicKey
  connection.remotePublicKey = randomBytes(32)
  return connection
}

class MockInviteApi extends TypedEmitter {
  /** @type {string} */
  #projectId

  /**
   * @param {{ projectId?: string }} [opts]
   */
  constructor({ projectId = randomBytes(20).toString('hex') } = {}) {
    super()
    this.#projectId = projectId
  }

  /**
   * @param {Pick<Invite, 'inviteId'>} _invite
   * @returns {Promise<string>}
   */
  async accept(_invite) {
    return this.#projectId
  }
}

/**
 * A scripted stand-in for RemoteDiscovery: records calls and lets the test
 * control when the invitor acks the redeem and when it admits or denies.
 *
 * @param {RemoteAuthedNoiseStream} connection
 */
function mockDiscovery(connection) {
  /** @type {string[]} */
  const connectCalls = []
  /** @type {string[]} */
  const disconnectCalls = []
  /** @type {string[]} */
  const leaveCalls = []
  /** @type {Redeem[]} */
  const redeemCalls = []
  const redeemAcked = pDefer()
  const admission = pDefer()
  // The test may never settle these
  redeemAcked.promise.catch(() => {})
  admission.promise.catch(() => {})

  /** @type {JoinerDiscovery} */
  const discovery = {
    async connectPeer(swarmPublicKeyHex) {
      connectCalls.push(swarmPublicKeyHex)
      return connection
    },
    async redeem(_connection, redeem) {
      redeemCalls.push(redeem)
      await redeemAcked.promise
    },
    async waitForAdmission(_connection, { signal } = {}) {
      signal?.throwIfAborted()
      await Promise.race([
        admission.promise,
        new Promise((_, reject) =>
          signal?.addEventListener('abort', () => reject(signal.reason), {
            once: true,
          })
        ),
      ])
    },
    async disconnectPeer(publicKeyHex) {
      disconnectCalls.push(publicKeyHex)
    },
    leavePeer(swarmPublicKeyHex) {
      leaveCalls.push(swarmPublicKeyHex)
    },
  }

  return {
    discovery,
    connectCalls,
    disconnectCalls,
    leaveCalls,
    redeemCalls,
    ackRedeem: () => redeemAcked.resolve(),
    admit: () => admission.resolve(),
    /** @param {Error} err */
    deny: (err) => admission.reject(err),
  }
}

/**
 * @param {Buffer} inviteId
 * @param {Buffer} swarmPublicKey
 */
function testUrl(inviteId, swarmPublicKey) {
  return makeInviteURL({
    inviteIdString: inviteId.toString('hex'),
    swarmPublicKey: swarmPublicKey.toString('hex'),
    invitorName: 'invitor',
    projectName: 'project',
    expiresAt: Date.now() + 60_000,
    roleId: MEMBER_ROLE_ID,
  })
}

/**
 * @param {InviteLinkJoiner} joiner
 * @param {JoinRequestUpdate['status']} status
 */
function onStatus(joiner, status) {
  return pEvent(joiner, 'join-request-update', {
    timeout: 1000,
    filter: (update) => update.status === status,
  })
}

test('happy path: connect, redeem, acked, admitted, invited, complete', async () => {
  const swarmPublicKey = randomBytes(32)
  const invitorIdentity = randomBytes(32)
  const inviteId = randomBytes(32)
  const projectId = randomBytes(20).toString('hex')
  const url = testUrl(inviteId, swarmPublicKey)

  const connection = mockConnection(invitorIdentity)
  const inviteApi = new MockInviteApi({ projectId })
  const mock = mockDiscovery(connection)

  const joiner = new InviteLinkJoiner({
    discovery: mock.discovery,
    inviteApi: /** @type {InviteApi} */ (/** @type {unknown} */ (inviteApi)),
    getDeviceInfo: () => ({ name: 'invitee', deviceType: 'mobile' }),
  })

  /** @type {JoinRequestUpdate[]} */
  const updates = []
  joiner.on('join-request-update', (update) => updates.push(update))

  const onConnected = onStatus(joiner, 'connected')
  const onRequested = onStatus(joiner, 'requested')
  const onAccepted = onStatus(joiner, 'accepted')
  const onCompleted = onStatus(joiner, 'completed')

  const joinRequest = joiner.createJoinRequest(url)
  assert.equal(joinRequest.status, 'connecting')
  assert.equal(joinRequest.inviteId, inviteId.toString('hex'))

  await onConnected
  assert.deepEqual(mock.connectCalls, [swarmPublicKey.toString('hex')])

  // Redeem was sent with our device info, but not acked yet
  assert.equal(mock.redeemCalls.length, 1)
  assert.ok(mock.redeemCalls[0].inviteId.equals(inviteId))
  assert.equal(mock.redeemCalls[0].deviceName, 'invitee')
  assert.equal(mock.redeemCalls[0].deviceType, 'mobile')
  assert.equal(joinRequest.status, 'connected')

  // Invitor acks: we are now waiting on a human
  mock.ackRedeem()
  await onRequested

  // Invitor admits
  mock.admit()
  await onAccepted

  // Invitor sends the invite over RPC
  inviteApi.emit('invite-received', {
    invitorDeviceId: invitorIdentity.toString('hex'),
    inviteId: inviteId.toString('hex'),
  })

  const completed = await onCompleted
  assert.equal(completed.projectId, projectId)

  assert.deepEqual(
    updates.map((u) => u.status),
    ['connecting', 'connected', 'requested', 'accepted', 'completed']
  )

  // Join request is removed and we stop redialling the invitor
  assert.throws(() => joiner.getJoinRequestById(inviteId.toString('hex')), {
    code: 'JOIN_REQUEST_NOT_FOUND_ERROR',
  })
  assert.deepEqual(mock.leaveCalls, [swarmPublicKey.toString('hex')])
  assert.deepEqual(mock.disconnectCalls, [])
})

test('invite received before admission resolves is not missed', async () => {
  const swarmPublicKey = randomBytes(32)
  const invitorIdentity = randomBytes(32)
  const inviteId = randomBytes(32)
  const url = testUrl(inviteId, swarmPublicKey)

  const connection = mockConnection(invitorIdentity)
  const inviteApi = new MockInviteApi()
  const mock = mockDiscovery(connection)

  const joiner = new InviteLinkJoiner({
    discovery: mock.discovery,
    inviteApi: /** @type {InviteApi} */ (/** @type {unknown} */ (inviteApi)),
    getDeviceInfo: () => ({}),
  })

  const onRequested = onStatus(joiner, 'requested')
  const onCompleted = onStatus(joiner, 'completed')
  joiner.createJoinRequest(url)
  mock.ackRedeem()
  await onRequested

  // Invite arrives on the same tick as the admit
  inviteApi.emit('invite-received', {
    invitorDeviceId: invitorIdentity.toString('hex'),
    inviteId: inviteId.toString('hex'),
  })
  mock.admit()

  await onCompleted
})

test('denied by invitor fails with the deny reason', async () => {
  const swarmPublicKey = randomBytes(32)
  const inviteId = randomBytes(32)
  const url = testUrl(inviteId, swarmPublicKey)
  const connection = mockConnection(randomBytes(32))
  const mock = mockDiscovery(connection)

  const joiner = new InviteLinkJoiner({
    discovery: mock.discovery,
    inviteApi: /** @type {InviteApi} */ (
      /** @type {unknown} */ (new MockInviteApi())
    ),
    getDeviceInfo: () => ({}),
  })

  const onFailed = onStatus(joiner, 'failed')
  joiner.createJoinRequest(url)
  mock.ackRedeem()
  mock.deny(new InviteDeniedByInviterError({ reason: 'invitor_denied' }))

  const failed = await onFailed
  assert.equal(failed.error?.name, InviteDeniedByInviterError.name)
  assert.deepEqual(mock.disconnectCalls, [swarmPublicKey.toString('hex')])
  assert.deepEqual(mock.leaveCalls, [swarmPublicKey.toString('hex')])
})

test('connection closed before decision fails with a connection error', async () => {
  const swarmPublicKey = randomBytes(32)
  const inviteId = randomBytes(32)
  const url = testUrl(inviteId, swarmPublicKey)
  const connection = mockConnection(randomBytes(32))
  const mock = mockDiscovery(connection)

  const joiner = new InviteLinkJoiner({
    discovery: mock.discovery,
    inviteApi: /** @type {InviteApi} */ (
      /** @type {unknown} */ (new MockInviteApi())
    ),
    getDeviceInfo: () => ({}),
  })

  const onFailed = onStatus(joiner, 'failed')
  joiner.createJoinRequest(url)
  mock.ackRedeem()
  mock.deny(new InviteRedeemConnectionClosedError())

  const failed = await onFailed
  assert.equal(failed.error?.name, InviteConnectionError.name)
  assert.equal(
    /** @type {any} */ (failed.error).cause?.code,
    InviteRedeemConnectionClosedError.code
  )
})

test('cancelling while waiting for the invitor to decide', async () => {
  const swarmPublicKey = randomBytes(32)
  const inviteId = randomBytes(32)
  const url = testUrl(inviteId, swarmPublicKey)
  const connection = mockConnection(randomBytes(32))
  const mock = mockDiscovery(connection)

  const joiner = new InviteLinkJoiner({
    discovery: mock.discovery,
    inviteApi: /** @type {InviteApi} */ (
      /** @type {unknown} */ (new MockInviteApi())
    ),
    getDeviceInfo: () => ({}),
  })

  const onRequested = onStatus(joiner, 'requested')
  const onFailed = onStatus(joiner, 'failed')
  joiner.createJoinRequest(url)
  mock.ackRedeem()
  await onRequested

  joiner.cancelJoinRequest(inviteId.toString('hex'))

  const failed = await onFailed
  assert.equal(failed.error?.name, JoinProjectCancelledError.name)
  assert.deepEqual(mock.disconnectCalls, [swarmPublicKey.toString('hex')])
})
