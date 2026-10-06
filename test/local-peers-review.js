// Review reproductions for digidem/comapeo-core#1254 (LocalPeers trust).
// Each test here encodes the behaviour we expect and is EXPECTED TO FAIL on
// the PR head until the corresponding issue is fixed.

import test from 'node:test'
import assert from 'node:assert/strict'
import { once } from 'events'
import { LocalPeers } from '../src/local-peers.js'
import { replicate } from './helpers/local-peers.js'

test('REVIEW: trustPeer rejects for a peer that has already disconnected', async () => {
  const r1 = new LocalPeers()
  const r2 = new LocalPeers()

  const destroy = replicate(r1, r2, { isTrusted1: false, isTrusted2: true })
  const [[peerFromR1]] = await once(r1, 'peers')
  assert.equal(peerFromR1.isTrusted, false)

  const onRemove = once(r1, 'peer-remove')
  await destroy()
  await onRemove

  /** @type {import('../src/local-peers.js').PeerInfo[]} */
  const trustedEvents = []
  r1.on('peer-trusted', (peer) => trustedEvents.push(peer))

  // MemberApi.acceptInviteLinkRequest relies on this rejecting to detect a
  // peer that disconnected after redeeming an invite.
  await assert.rejects(
    r1.trustPeer(peerFromR1.deviceId),
    'trusting a disconnected peer should fail'
  )
  assert.equal(
    trustedEvents.length,
    0,
    `peer-trusted emitted for a peer with status '${trustedEvents[0]?.status}'`
  )
})
