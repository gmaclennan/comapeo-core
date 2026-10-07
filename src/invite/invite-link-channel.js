import { TypedEmitter } from 'tiny-typed-emitter'
import cenc from 'compact-encoding'
import pDefer from 'p-defer'
import timingSafeEqual from 'string-timing-safe-equal'
import {
  Redeem,
  RedeemAck,
  Admit,
  Deny,
  DenyAck,
} from '../generated/invite-link.js'
import { Logger } from '../logger.js'
import { noop, timeoutPromise } from '../utils.js'
import { InviteRedeemConnectionClosedError, TimeoutError } from '../errors.js'

/** @import Protomux from 'protomux' */
/** @import { DeferredPromise } from 'p-defer' */

export const INVITE_LINK_PROTOCOL = 'comapeo/invite-link'

/**
 * How long to wait for the peer to acknowledge a Redeem or Deny before giving
 * up. Acks are sent by the peer's application as soon as it has handled the
 * message, so this only needs to cover network latency and a busy event loop,
 * not any human decision.
 */
export const INVITE_LINK_ACK_TIMEOUT_MS = 10_000

// Message IDs are positional on the wire: never re-order or re-use these.
const MESSAGE_TYPES = /** @type {const} */ ({
  Redeem: 0,
  RedeemAck: 1,
  Admit: 2,
  Deny: 3,
  DenyAck: 4,
})

/**
 * @typedef {object} InviteLinkChannelEvents
 * @property {(redeem: Redeem) => void} redeem Peer is asking to redeem an invite link (ack has already been sent)
 * @property {(admit: Admit) => void} admit Peer says we are admitted
 * @property {(deny: Deny) => void} deny Peer has denied us (ack has already been sent)
 * @property {() => void} close Channel closed (peer closed it, rejected it, or the stream ended)
 */

/**
 * The `comapeo/invite-link` protocol: the only thing an authenticated but
 * not-yet-admitted remote peer can talk to us about. It is a request/response
 * exchange where the response (Admit or Deny) may take as long as a human
 * takes to decide, so Redeem and Deny are acknowledged separately to tell
 * "received, waiting for a decision" apart from "never arrived".
 *
 * One instance per connection. Both sides create it right after the NOISE
 * stream opens; protomux pairs the two. The channel only moves messages: what
 * an Admit or Deny *means* for the connection is decided by RemoteDiscovery,
 * which checks them against the request it actually sent.
 *
 * @extends {TypedEmitter<InviteLinkChannelEvents>}
 */
export class InviteLinkChannel extends TypedEmitter {
  #channel
  #opened = pDefer()
  #closed = false
  #acceptRedeem
  /** @type {Map<'RedeemAck' | 'DenyAck', Set<{ inviteId: Buffer, deferred: DeferredPromise<void> }>>} */
  #ackWaiters = new Map()
  /** @type {Set<DeferredPromise<void>>} */
  #drainWaiters = new Set()
  #l

  /**
   * @param {Protomux<any>} protomux
   * @param {object} [opts]
   * @param {Logger} [opts.logger]
   * @param {(redeem: Redeem) => boolean} [opts.acceptRedeem] Called before acknowledging a Redeem; return false to drop it unacknowledged (e.g. the peer has not proven its identity yet)
   */
  constructor(protomux, { logger, acceptRedeem = () => true } = {}) {
    super()
    this.#l = Logger.create('inviteLink', logger)
    this.#acceptRedeem = acceptRedeem

    /** @type {Parameters<typeof Protomux.prototype.createChannel>[0]['messages']} */
    const messages = []
    messages[MESSAGE_TYPES.Redeem] = {
      encoding: cenc.raw,
      onmessage: /** @param {Buffer} buf */ (buf) =>
        this.#handleRedeem(Redeem.decode(buf)),
    }
    messages[MESSAGE_TYPES.RedeemAck] = {
      encoding: cenc.raw,
      onmessage: /** @param {Buffer} buf */ (buf) =>
        this.#receiveAck('RedeemAck', RedeemAck.decode(buf)),
    }
    messages[MESSAGE_TYPES.Admit] = {
      encoding: cenc.raw,
      onmessage: /** @param {Buffer} buf */ (buf) =>
        this.emit('admit', Admit.decode(buf)),
    }
    messages[MESSAGE_TYPES.Deny] = {
      encoding: cenc.raw,
      onmessage: /** @param {Buffer} buf */ (buf) =>
        this.#handleDeny(Deny.decode(buf)),
    }
    messages[MESSAGE_TYPES.DenyAck] = {
      encoding: cenc.raw,
      onmessage: /** @param {Buffer} buf */ (buf) =>
        this.#receiveAck('DenyAck', DenyAck.decode(buf)),
    }

    const channel = protomux.createChannel({
      protocol: INVITE_LINK_PROTOCOL,
      messages,
      onopen: () => this.#opened.resolve(),
      onclose: () => this.#handleClose(),
      ondrain: () => {
        for (const deferred of this.#drainWaiters) deferred.resolve()
        this.#drainWaiters.clear()
      },
    })
    if (!channel) {
      // Stream already destroyed, or a channel for this protocol already
      // exists on this protomux (which would be a bug in the caller)
      this.#channel = null
      this.#handleClose()
      return
    }
    this.#channel = channel
    channel.open()
  }

  get closed() {
    return this.#closed
  }

  /**
   * Ask the peer to redeem an invite link. Resolves once the peer has
   * acknowledged the request, i.e. it has recorded the request and is waiting
   * for a decision. Rejects with a TimeoutError if no ack arrives, or with
   * InviteRedeemConnectionClosedError if the channel closes first.
   *
   * @param {Redeem} redeem
   * @returns {Promise<void>}
   */
  async sendRedeem(redeem) {
    await this.#send(MESSAGE_TYPES.Redeem, Redeem.encode(redeem).finish())
    await this.#waitForAck('RedeemAck', redeem.inviteId)
    this.#l.log('redeem %h acknowledged', redeem.inviteId)
  }

  /**
   * Tell the peer it is admitted. Synchronous: the bytes are handed to the
   * stream before this returns, so the caller can set up what the peer's next
   * message depends on (its RPC open) in the same tick. There is no ack: the
   * next thing that happens is the regular invite over the RPC channel, which
   * has its own acknowledgement.
   *
   * @param {Admit} admit
   * @returns {boolean} false if the channel is closed
   */
  trySendAdmit(admit) {
    if (this.#closed || !this.#channel) return false
    this.#channel.messages[MESSAGE_TYPES.Admit].send(
      Buffer.from(Admit.encode(admit).finish())
    )
    this.#l.log('sent admit for %h', admit.inviteId)
    return true
  }

  /**
   * Tell the peer it is denied. Resolves once the peer has acknowledged the
   * deny, or after the ack timeout, so that the caller can close the
   * connection knowing the peer had a chance to read the reason.
   *
   * @param {Deny} deny
   * @returns {Promise<boolean>} true if the peer acknowledged the deny
   */
  async sendDeny(deny) {
    await this.#send(MESSAGE_TYPES.Deny, Deny.encode(deny).finish())
    try {
      await this.#waitForAck('DenyAck', deny.inviteId)
      return true
    } catch (e) {
      this.#l.log('deny for %h was not acknowledged: %s', deny.inviteId, e)
      return false
    }
  }

  close() {
    this.#channel?.close()
  }

  /** @param {Redeem} redeem */
  #handleRedeem(redeem) {
    if (!this.#acceptRedeem(redeem)) {
      this.#l.log('dropping redeem %h: not accepted', redeem.inviteId)
      return
    }
    // Ack first: the ack means "received", not "decided"
    this.#send(
      MESSAGE_TYPES.RedeemAck,
      RedeemAck.encode({ inviteId: redeem.inviteId }).finish()
    )
      .then(() => this.emit('redeem', redeem))
      .catch((e) => this.#l.log('failed to ack redeem: %s', e))
  }

  /** @param {Deny} deny */
  #handleDeny(deny) {
    this.#send(
      MESSAGE_TYPES.DenyAck,
      DenyAck.encode({ inviteId: deny.inviteId }).finish()
    )
      .then(() => this.emit('deny', deny))
      .catch((e) => this.#l.log('failed to ack deny: %s', e))
  }

  #handleClose() {
    if (this.#closed) return
    this.#closed = true
    this.#opened.reject(new InviteRedeemConnectionClosedError())
    this.#opened.promise.catch(noop)
    for (const waiters of this.#ackWaiters.values()) {
      for (const { deferred } of waiters) {
        deferred.reject(new InviteRedeemConnectionClosedError())
      }
    }
    this.#ackWaiters.clear()
    for (const deferred of this.#drainWaiters) {
      deferred.reject(new InviteRedeemConnectionClosedError())
    }
    this.#drainWaiters.clear()
    this.emit('close')
  }

  /**
   * @param {number} messageType
   * @param {Uint8Array} encoded
   */
  async #send(messageType, encoded) {
    if (this.#closed || !this.#channel) {
      throw new InviteRedeemConnectionClosedError()
    }
    await timeoutPromise(this.#opened.promise, {
      milliseconds: INVITE_LINK_ACK_TIMEOUT_MS,
    })
    const didWrite = this.#channel.messages[messageType].send(
      Buffer.from(encoded)
    )
    if (didWrite) return
    const deferred = pDefer()
    this.#drainWaiters.add(deferred)
    await deferred.promise
  }

  /**
   * @param {'RedeemAck' | 'DenyAck'} type
   * @param {Buffer} inviteId
   */
  async #waitForAck(type, inviteId) {
    if (this.#closed) throw new InviteRedeemConnectionClosedError()
    /** @type {DeferredPromise<void>} */
    const deferred = pDefer()
    const waiter = { inviteId, deferred }
    const waiters = this.#ackWaiters.get(type) || new Set()
    waiters.add(waiter)
    this.#ackWaiters.set(type, waiters)
    try {
      await timeoutPromise(deferred.promise, {
        milliseconds: INVITE_LINK_ACK_TIMEOUT_MS,
      })
    } catch (e) {
      if (e instanceof TimeoutError) {
        throw new TimeoutError(`Timed out waiting for ${type}`)
      }
      throw e
    } finally {
      waiters.delete(waiter)
    }
  }

  /**
   * @param {'RedeemAck' | 'DenyAck'} type
   * @param {{ inviteId: Buffer }} ack
   */
  #receiveAck(type, ack) {
    const waiters = this.#ackWaiters.get(type)
    if (!waiters) return
    for (const waiter of waiters) {
      if (timingSafeEqual(waiter.inviteId, ack.inviteId)) {
        waiter.deferred.resolve()
      }
    }
  }
}
