// Review reproductions for digidem/comapeo-core#1254 (InviteLinkJoiner).
// Each test here encodes the behaviour we expect and is EXPECTED TO FAIL on
// the PR head until the corresponding issue is fixed.

import test from 'node:test'
import { TypedEmitter } from 'tiny-typed-emitter'
import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import { Transform } from 'streamx'
import { pEvent } from 'p-event'
import { setTimeout as sleep } from 'node:timers/promises'

import { InviteLinkJoiner } from '../src/invite/invite-link-joiner.js'
import { makeInviteURL } from '../src/invite/invite-urls.js'
import { MEMBER_ROLE_ID } from '../src/roles.js'

/** @import { RemoteAuthedNoiseStream } from '../src/discovery/remote-discovery.js' */
/** @import { Invite, InviteApi } from '../src/invite/invite-api.js' */

/** @returns {RemoteAuthedNoiseStream} */
function mockConnection() {
  const connection = /** @type {RemoteAuthedNoiseStream} */ (
    /** @type {unknown} */ (new Transform())
  )
  connection.authenticatedPublicKey = randomBytes(32)
  connection.isTrusted = true
  connection.remotePublicKey = randomBytes(32)
  return connection
}

class MockInviteApi extends TypedEmitter {
  /**
   * @param {Pick<Invite, 'inviteId'>} _invite
   * @returns {Promise<string>}
   */
  async accept(_invite) {
    return randomBytes(20).toString('hex')
  }
}

/** @param {Buffer} inviteId */
function makeUrl(inviteId) {
  return makeInviteURL({
    inviteIdString: inviteId.toString('hex'),
    swarmPublicKey: randomBytes(32).toString('hex'),
    invitorName: 'invitor',
    projectName: 'project',
    expiresAt: Date.now() + 60_000,
    roleId: MEMBER_ROLE_ID,
  })
}

/**
 * @param {import('../src/invite/invite-link-joiner.js').InviteLinkJoiner} joiner
 * @param {string} inviteId
 * @param {import('../src/invite/invite-link-joiner.js').JoinRequest['status']} status
 */
function onStatus(joiner, inviteId, status) {
  return pEvent(joiner, 'join-request-update', {
    timeout: 2000,
    filter: (update) =>
      update.inviteId === inviteId && update.status === status,
  })
}

test('REVIEW: cancelling then retrying a join request keeps the retry tracked', async (t) => {
  const inviteId = randomBytes(32)
  const inviteIdHex = inviteId.toString('hex')
  const url = makeUrl(inviteId)
  const inviteApi = new MockInviteApi()

  const joiner = new InviteLinkJoiner({
    connectPeer: async () => {
      await sleep(20)
      return mockConnection()
    },
    disconnectPeer: async () => {
      await sleep(5)
    },
    sendRedeemInviteOverInternet: async () => {},
    inviteApi: /** @type {InviteApi} */ (/** @type {unknown} */ (inviteApi)),
  })
  t.after(() => {
    try {
      joiner.cancelJoinRequest(inviteIdHex)
    } catch {
      // nothing left to cancel
    }
  })

  const firstConnected = onStatus(joiner, inviteIdHex, 'connected')
  joiner.createJoinRequest(url)
  await firstConnected

  // User cancels while waiting for the invite, then immediately retries
  const firstFailed = onStatus(joiner, inviteIdHex, 'failed')
  joiner.cancelJoinRequest(inviteIdHex)
  const retry = joiner.createJoinRequest(url)
  await firstFailed

  const retryConnected = onStatus(joiner, inviteIdHex, 'connected')
  await retryConnected
  // Give the cancelled flow's cleanup a chance to run
  await sleep(50)

  assert.deepEqual(
    joiner.getJoinRequests(),
    [retry],
    'the retried join request is still tracked after the cancelled flow cleans up'
  )
  assert.doesNotThrow(
    () => joiner.cancelJoinRequest(inviteIdHex),
    'the retried join request can be cancelled'
  )
})

test('REVIEW: failed join requests do not leak invite-received listeners', async () => {
  const inviteApi = new MockInviteApi()
  /** @type {RemoteAuthedNoiseStream | undefined} */
  let connection
  const joiner = new InviteLinkJoiner({
    connectPeer: async () => {
      connection = mockConnection()
      return connection
    },
    disconnectPeer: async () => {},
    sendRedeemInviteOverInternet: async () => {},
    inviteApi: /** @type {InviteApi} */ (/** @type {unknown} */ (inviteApi)),
  })

  const ATTEMPTS = 5
  for (let i = 0; i < ATTEMPTS; i++) {
    const inviteId = randomBytes(32)
    const inviteIdHex = inviteId.toString('hex')
    const connected = onStatus(joiner, inviteIdHex, 'connected')
    const failed = onStatus(joiner, inviteIdHex, 'failed')
    joiner.createJoinRequest(makeUrl(inviteId))
    await connected
    // Invitor drops the connection before any invite arrives
    connection?.destroy()
    await failed
  }
  await sleep(10)

  assert.equal(joiner.getJoinRequests().length, 0, 'no join requests pending')
  assert.equal(
    inviteApi.listenerCount('invite-received'),
    0,
    `invite-received listeners left on InviteApi after ${ATTEMPTS} failed joins`
  )
})
