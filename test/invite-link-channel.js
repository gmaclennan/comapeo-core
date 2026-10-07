import test from 'node:test'
import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import { Duplex } from 'streamx'
import Protomux from 'protomux'
import { pEvent } from 'p-event'

import {
  InviteLinkChannel,
  INVITE_LINK_PROTOCOL,
} from '../src/invite/invite-link-channel.js'
import { InviteRedeemConnectionClosedError } from '../src/errors.js'

/** @import { Redeem, Admit, Deny } from '../src/generated/invite-link.js' */

/**
 * Creates a pair of connected duplex streams that pipe data to each other.
 * @returns {[Duplex, Duplex]}
 */
function createStreamPair() {
  const s1 = new Duplex({
    read() {},
    write(chunk, cb) {
      if (s2 && !s2.destroyed) s2.push(chunk)
      cb()
    },
  })
  const s2 = new Duplex({
    read() {},
    write(chunk, cb) {
      if (s1 && !s1.destroyed) s1.push(chunk)
      cb()
    },
  })
  return [s1, s2]
}

function setup() {
  const [s1, s2] = createStreamPair()
  const invitee = new InviteLinkChannel(Protomux.from(s1))
  const invitor = new InviteLinkChannel(Protomux.from(s2))
  return { invitee, invitor, s1, s2 }
}

test('redeem is acknowledged by the peer application before it is emitted', async () => {
  const { invitee, invitor } = setup()
  const inviteId = randomBytes(32)

  const onRedeem = /** @type {Promise<Redeem>} */ (
    /** @type {unknown} */ (pEvent(invitor, 'redeem', { timeout: 1000 }))
  )
  await invitee.sendRedeem({
    inviteId,
    deviceName: 'invitee',
    deviceType: 'mobile',
  })
  const redeem = await onRedeem
  assert.ok(redeem.inviteId.equals(inviteId))
  assert.equal(redeem.deviceName, 'invitee')
  assert.equal(redeem.deviceType, 'mobile')
})

test('admit is delivered', async () => {
  const { invitee, invitor } = setup()
  const inviteId = randomBytes(32)

  const onAdmit = /** @type {Promise<Admit>} */ (
    /** @type {unknown} */ (pEvent(invitee, 'admit', { timeout: 1000 }))
  )
  await invitor.sendAdmit({ inviteId })
  const admit = await onAdmit
  assert.ok(admit.inviteId.equals(inviteId))
})

test('deny is acknowledged, then the invitor may close', async () => {
  const { invitee, invitor } = setup()
  const inviteId = randomBytes(32)

  const onDeny = /** @type {Promise<Deny>} */ (
    /** @type {unknown} */ (pEvent(invitee, 'deny', { timeout: 1000 }))
  )
  const acked = await invitor.sendDeny({ inviteId, reason: 'invitor_denied' })
  assert.equal(acked, true, 'invitee acknowledged the deny')
  const deny = await onDeny
  assert.equal(deny.reason, 'invitor_denied')
})

test('pending redeem rejects when the stream is destroyed', async () => {
  const [s1] = createStreamPair()
  const channel = new InviteLinkChannel(Protomux.from(s1))

  const redeemPromise = channel.sendRedeem({
    inviteId: randomBytes(32),
    deviceName: '',
    deviceType: 'device_type_unspecified',
  })
  redeemPromise.catch(() => {})
  s1.destroy()

  await assert.rejects(redeemPromise, {
    code: InviteRedeemConnectionClosedError.code,
  })
  assert.equal(channel.closed, true)
})

test('open is rejected by a peer that does not speak the protocol', async () => {
  const [s1, s2] = createStreamPair()
  // Peer attaches protomux but never creates or pairs the invite-link protocol
  Protomux.from(s2)
  const channel = new InviteLinkChannel(Protomux.from(s1))

  await pEvent(channel, 'close', { timeout: 1000 })
  assert.equal(channel.closed, true)
})

test('protocol name is stable', () => {
  // The protocol name is part of the wire format: changing it is a breaking change
  assert.equal(INVITE_LINK_PROTOCOL, 'comapeo/invite-link')
})
