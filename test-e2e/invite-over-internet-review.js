// Review reproductions for digidem/comapeo-core#1254 (invite over internet).
// Each test here encodes the behaviour we expect and is EXPECTED TO FAIL on
// the PR head until the corresponding issue is fixed.

import test from 'node:test'
import assert from 'node:assert/strict'
import createTestnet from 'hyperdht/testnet.js'
import { KeyManager } from '@comapeo/crypto'
import { pEvent } from 'p-event'

import { createManager, createManagers } from './utils.js'
import { MEMBER_ROLE_ID } from '../src/roles.js'
import { RemoteDiscovery } from '../src/discovery/remote-discovery.js'
import { LocalPeers } from '../src/local-peers.js'
import { getDeviceId } from '../src/utils.js'
import { InviteResponse_Decision } from '../src/generated/rpc.js'
import { parseInviteURL } from '../src/invite/invite-urls.js'
import { AlreadyInvitingError } from '../src/errors.js'

test('REVIEW: a peer that rejects the invite does not stay trusted and connected', async (t) => {
  const testnet = await createTestnet(2)
  t.after(() => testnet.destroy())

  const UNTRUSTED_TIMEOUT = 1000

  const manager = createManager('invitor', t, {
    swarm: { dht: testnet.nodes[0] },
    untrustedTimeout: UNTRUSTED_TIMEOUT,
  })
  await manager.setDeviceInfo({ name: 'invitor', deviceType: 'desktop' })

  const projectId = await manager.createProject({ name: 'Test Project' })
  const project = await manager.getProject(projectId)

  const url = await project.$member.createInviteLink({
    roleId: MEMBER_ROLE_ID,
  })
  const { inviteIdString, swarmPublicKey } = parseInviteURL(url)

  // A minimal peer that redeems the invite and then answers REJECT
  const keyManager = new KeyManager(KeyManager.generateRootKey())
  const remoteDiscovery = new RemoteDiscovery({
    identityKeypair: keyManager.getIdentityKeypair(),
    deriveSwarmIdentityKeypair: () =>
      keyManager.deriveSwarmIdentity(new Date()),
    swarm: { dht: testnet.nodes[0] },
  })
  t.after(() => remoteDiscovery.close())
  const localPeers = new LocalPeers()
  remoteDiscovery.on('connection', (noiseStream) => {
    localPeers.connect(noiseStream, noiseStream.isTrusted)
  })
  localPeers.on('invite', (peerId, invite) => {
    localPeers
      .sendInviteResponse(peerId, {
        inviteId: invite.inviteId,
        decision: InviteResponse_Decision.REJECT,
      })
      .catch(() => {})
  })

  const connection = await remoteDiscovery.connectPeer(swarmPublicKey, {
    timeout: 5000,
  })

  const onRedeemAttempt = pEvent(manager, 'invite-link-join-request', {
    multiArgs: true,
    timeout: 5000,
  })
  await localPeers.sendRedeemInviteOverInternet(manager.deviceId, {
    inviteId: Buffer.from(inviteIdString, 'hex'),
  })
  const [, deviceId, redeemInviteId] = /** @type {[string, string, string]} */ (
    /** @type {unknown} */ (await onRedeemAttempt)
  )
  assert.equal(deviceId, getDeviceId(keyManager))

  const decision = await project.$member.acceptInviteLinkRequest(
    redeemInviteId,
    deviceId
  )
  assert.equal(decision, InviteResponse_Decision.REJECT)

  // The invitor trusted this peer in order to send the invite. Now that the
  // invite is over without the peer joining, it must not keep a trusted,
  // replicating connection open: expect it to be disconnected.
  await assert.doesNotReject(
    pEvent(connection, 'close', { timeout: UNTRUSTED_TIMEOUT + 4000 }),
    'connection to a peer that rejected the invite stays open'
  )
})

test('REVIEW: a second concurrent accept does not tear down the first invite', async (t) => {
  const [invitor, invitee] = await createManagers(
    2,
    t,
    'device_type_unspecified',
    { useTestnet: true }
  )

  const projectId = await invitor.createProject({ name: 'Mapeo' })
  const project = await invitor.getProject(projectId)
  const url = await project.$member.createInviteLink({
    roleId: MEMBER_ROLE_ID,
  })
  const { inviteIdString: inviteId } = parseInviteURL(url)

  const onRedeemAttempt = pEvent(invitor, 'invite-link-join-request', {
    multiArgs: true,
    rejectionEvents: ['invite-link-join-request-error'],
    timeout: 5000,
  })
  const onJoinDone = pEvent(invitee.inviteLinks, 'join-request-update', {
    timeout: 10_000,
    filter: (update) =>
      update.inviteId === inviteId &&
      (update.status === 'completed' || update.status === 'failed'),
  })

  invitee.inviteLinks.createJoinRequest(url)
  const [, deviceId, redeemInviteId] = /** @type {[string, string, string]} */ (
    /** @type {unknown} */ (await onRedeemAttempt)
  )

  // e.g. a double tap on "accept" in the UI
  const first = project.$member.acceptInviteLinkRequest(
    redeemInviteId,
    deviceId
  )
  const second = project.$member.acceptInviteLinkRequest(
    redeemInviteId,
    deviceId
  )

  await assert.rejects(
    second,
    { code: AlreadyInvitingError.code },
    'second accept is refused because an invite is already in flight'
  )
  const firstResult = await first.catch((e) => e)
  assert.equal(
    firstResult,
    InviteResponse_Decision.ACCEPT,
    `first accept should complete normally, got ${
      firstResult instanceof Error ? firstResult.message : firstResult
    }`
  )
  const update = await onJoinDone
  assert.equal(update.status, 'completed', `join ended as ${update.status}`)
  assert.equal(update.projectId, projectId)
})
