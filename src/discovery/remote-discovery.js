import { TypedEmitter } from 'tiny-typed-emitter'
import { Logger } from '../logger.js'
import Hyperswarm from 'hyperswarm'
import StartStopStateMachine from 'start-stop-state-machine'
import { pEvent, TimeoutError as EventTimeoutError } from 'p-event'
import Protomux from 'protomux'
import pDefer from 'p-defer'
import timingSafeEqual from 'string-timing-safe-equal'
import { noop } from '../utils.js'
import { abortable } from '../lib/abortable.js'
import { identityHandshake } from './identity-handshake.js'
import { InviteLinkChannel } from '../invite/invite-link-channel.js'
import {
  ensureKnownError,
  InviteDeniedByInviterError,
  InviteLinkNotRequestedError,
  InviteLinkRequestPendingError,
  InviteRedeemConnectionClosedError,
  SwarmNotInitializedError,
  TimeoutError,
  UnknownInviteIDError,
  UnknownPeerError,
} from '../errors.js'

/** @import {OpenedNoiseStream, AuthedNoiseStream} from '../lib/noise-secret-stream-helpers.js' */
/** @import {Keypair} from './local-discovery.js' */
/** @import {Redeem, Deny} from '../generated/invite-link.js' */
/** @import {DeferredPromise} from 'p-defer' */

// Re-export for consumers that import from this module
/** @typedef {AuthedNoiseStream} RemoteAuthedNoiseStream */

/**
 * @typedef {Object} DiscoveryEvents
 * @property {(connection: RemoteAuthedNoiseStream) => void} connection Emitted once a peer is admitted: it may now open the RPC channel and replicate
 * @property {(connection: RemoteAuthedNoiseStream) => void} authenticated Emitted once a peer has proven its identity, before it is admitted. Only the invite-link protocol is available on the connection at this point.
 * @property {(deviceId: string, redeem: Redeem) => void} redeem Emitted when an authenticated peer asks to redeem an invite link. The request has already been acknowledged; call `admit()` or `deny()` to decide. A peer may repeat a redeem (e.g. after a dropped connection), in which case this is emitted again for the same device and invite.
 * @property {(deviceId: string) => void} peer-closed Emitted when the last connection to an authenticated peer closes, whether or not it was admitted
 * @property {(error: Error) => void} error
 */

// Symbol for test-only access to internal methods
export const kTestOnlyHandleHyperswarmConnection = Symbol(
  'testOnlyHandleHyperswarmConnection'
)

/**
 * How long an authenticated peer has to redeem an invite link (or be admitted
 * for another reason) before we close the connection. Once a redeem has been
 * acknowledged the connection is kept open for as long as it takes the invitor
 * to decide.
 */
export const DEFAULT_ADMISSION_TIMEOUT = 16_000

/**
 * How long we remember that a device was admitted, so that a dropped and
 * redialled connection during an invite flow is re-admitted without a new
 * redeem, and replication can resume.
 */
const ADMITTED_TTL = 5 * 60_000

/**
 * @typedef {'handshaking' | 'authenticated' | 'admitted' | 'closed'} RemoteConnectionState
 */

/**
 * An invite link we have asked the peer on a connection to redeem, and the
 * peer's decision on it. A connection carries at most one request awaiting a
 * decision at a time.
 *
 * @typedef {object} InviteLinkRequest
 * @property {Buffer} inviteId
 * @property {DeferredPromise<void>} decision resolves on Admit, rejects on Deny, close or send failure
 * @property {boolean} settled
 */

/**
 * Per-connection state. A connection climbs
 * handshaking → authenticated → admitted, or is closed on the way.
 */
class RemoteConnection {
  /** @type {RemoteConnectionState} */
  state = 'handshaking'
  socket
  /** @type {Buffer | null} */
  identityPublicKey = null
  /** @type {InviteLinkChannel | null} */
  inviteLink = null
  /** @type {DeferredPromise<void>} resolves when authenticated, rejects on failure */
  authenticated = pDefer()
  /**
   * The invite we have asked this peer to redeem. Only an Admit or Deny that
   * matches it means anything: an Admit we never asked for is not admission.
   * @type {InviteLinkRequest | null}
   */
  outgoingRequest = null
  /**
   * The invite this peer has asked us to redeem (the latest one), so that
   * `admit()` and `deny()` only answer connections that actually asked.
   * @type {Buffer | null}
   */
  incomingInviteId = null
  /** @type {ReturnType<typeof setTimeout> | null} */
  #admissionTimer = null

  /** @param {OpenedNoiseStream} socket */
  constructor(socket) {
    this.socket = socket
    // This may legitimately never be awaited
    this.authenticated.promise.catch(noop)
  }

  get identityHex() {
    return this.identityPublicKey?.toString('hex')
  }

  /**
   * @param {number} ms
   * @param {() => void} onExpire
   */
  armAdmissionTimer(ms, onExpire) {
    this.disarmAdmissionTimer()
    this.#admissionTimer = setTimeout(onExpire, ms)
    this.#admissionTimer.unref()
  }

  disarmAdmissionTimer() {
    if (this.#admissionTimer !== null) clearTimeout(this.#admissionTimer)
    this.#admissionTimer = null
  }

  /** @param {Buffer} inviteId */
  startRequest(inviteId) {
    const decision = pDefer()
    decision.promise.catch(noop)
    this.outgoingRequest = { inviteId, decision, settled: false }
  }

  /** @param {Buffer} inviteId */
  hasPendingRequestFor(inviteId) {
    const request = this.outgoingRequest
    return (
      request !== null &&
      !request.settled &&
      timingSafeEqual(request.inviteId, inviteId)
    )
  }

  /** @param {Error} [error] */
  settleRequest(error) {
    const request = this.outgoingRequest
    if (!request || request.settled) return
    request.settled = true
    if (error) request.decision.reject(error)
    else request.decision.resolve()
  }

  /**
   * @param {Error} reason
   * @returns {boolean} false if already closed
   */
  close(reason) {
    if (this.state === 'closed') return false
    this.disarmAdmissionTimer()
    this.state = 'closed'
    this.authenticated.reject(reason)
    this.settleRequest(reason)
    this.inviteLink?.close()
    return true
  }
}

/**
 * Discovery and connection over the internet via Hyperswarm.
 *
 * Owns the whole pre-admission life of a remote connection:
 *
 * 1. NOISE handshake with an ephemeral swarm keypair (Hyperswarm)
 * 2. Identity handshake on the `comapeo/auth` channel, proving the stable
 *    identity key behind the swarm key
 * 3. Admission: the `comapeo/invite-link` channel is the only thing an
 *    authenticated peer can talk to us about until it is admitted, either by
 *    redeeming an invite link that the invitor accepts, or because we admitted
 *    this device recently and it has reconnected. Admission can only follow
 *    from a redeem *we* sent: an Admit the peer sends unasked is dropped and
 *    the connection closed.
 *
 * Only admitted connections are emitted as `connection`, so consumers (the
 * RPC layer and hypercore replication) never see a peer we have not decided
 * to talk to.
 *
 * A note on ordering: protomux processes every message in a received chunk
 * synchronously, and rejects an open for a protocol that has neither a
 * channel nor a pairing yet. So (a) every channel the peer may open before
 * admission is created up front, and (b) every state change that the peer's
 * *next* message may depend on happens synchronously inside the message
 * handler that triggers it, never in an `await` continuation. In particular
 * the transition to authenticated, and any admission that follows from it,
 * runs inside the identity-proof handler, so that the `mapeo/rpc` pairing that
 * LocalPeers registers on `connection` exists before the peer's RPC open can
 * be processed. For the same reason `admit()` writes Admit and admits on our
 * side in one tick.
 *
 * @extends {TypedEmitter<DiscoveryEvents>}
 */
export class RemoteDiscovery extends TypedEmitter {
  #l
  /** @type {Hyperswarm?} */
  #swarm = null
  #sm
  #identityKeypair
  #deriveSwarmIdentityKeypair
  /** @type {Keypair?} */
  #lastKeyPair = null
  #swarmOpts
  #admissionTimeout
  /** @type {Map<OpenedNoiseStream, RemoteConnection>} */
  #connections = new Map()
  /** @type {Map<string, number>} identity hex → admitted-until timestamp */
  #admittedDevices = new Map()

  /**
   * @param {Object} opts
   * @param {Keypair} opts.identityKeypair
   * @param {() => Keypair} opts.deriveSwarmIdentityKeypair
   * @param {number} [opts.admissionTimeout] ms an authenticated peer has to redeem an invite before being disconnected
   * @param {Logger} [opts.logger]
   * @param {object} [opts.swarm] - Optional Hyperswarm constructor overrides (e.g. { dht })
   */
  constructor({
    identityKeypair,
    deriveSwarmIdentityKeypair,
    admissionTimeout = DEFAULT_ADMISSION_TIMEOUT,
    logger,
    swarm: swarmOpts,
  }) {
    super()
    this.#l = Logger.create('RemoteDiscovery', logger)
    this.#identityKeypair = identityKeypair
    this.#deriveSwarmIdentityKeypair = deriveSwarmIdentityKeypair
    this.#admissionTimeout = admissionTimeout
    this.#swarmOpts = swarmOpts
    this.#sm = new StartStopStateMachine({
      start: this.#start.bind(this),
      stop: this.#stop.bind(this),
    })
  }

  async #start() {
    const keyPair = this.#deriveSwarmIdentityKeypair()
    if (this.#swarm) {
      if (
        !this.#lastKeyPair ||
        this.#lastKeyPair.publicKey.equals(keyPair.publicKey)
      ) {
        this.#l.log('Resuming swarm')
        await this.#swarm.resume()
        return
      } else {
        this.#l.log('Swarm key changed, destroying old swarm')
        await this.#swarm.destroy()
      }
    }
    this.#l.log('Initializing swarm')
    this.#lastKeyPair = keyPair
    const swarm = new Hyperswarm({
      keyPair,
      maxPeers: 16,
      ...this.#swarmOpts,
    })
    // @ts-expect-error Hyperswarm lacks the expected utility class to mark the stream as opened
    swarm.on('connection', this.#handleHyperswarmConnection.bind(this))
    this.#l.log('Starting listen')
    await swarm.listen()
    this.#l.log('Listening')
    await swarm.resume()
    this.#swarm = swarm
  }

  /**
   * Start listening for incoming connections
   */
  async start() {
    return this.#sm.start()
  }

  /**
   * @param {object} [opts]
   * @param {boolean} [opts.force=false] Force-close open connections
   * @returns {Promise<void>}
   */
  async stop(opts) {
    return this.#sm.stop(opts)
  }

  /**
   * @param {object} [opts]
   * @param {boolean} [opts.force=false] Force-close open connections
   */
  async #stop(opts) {
    this.#l.log('Suspending swarm')
    await this.#swarm?.suspend()
    if (opts?.force && this.#connections.size) {
      this.#l.log('Force closing existing connections')
      for (const { socket } of this.#connections.values()) {
        socket.end()
      }
    }
  }

  async close() {
    for (const conn of this.#connections.values()) {
      conn.disarmAdmissionTimer()
    }
    await this.#swarm?.destroy()
    this.#l.log('Closed swarm')
  }

  /**
   * @param {OpenedNoiseStream} socket
   */
  async [kTestOnlyHandleHyperswarmConnection](socket) {
    return this.#handleHyperswarmConnection(socket)
  }

  /**
   * Close every connection to a device.
   * @param {string} deviceId identity public key as hex
   */
  async disconnectPeer(deviceId) {
    const conns = this.#findByIdentity(deviceId)
    if (conns.length === 0) {
      this.#l.log('Cannot disconnect from peer %S, not connected', deviceId)
      return
    }
    this.#l.log('Disconnecting from peer %S', deviceId)
    await Promise.all(
      conns.map(async ({ socket }) => {
        socket.end()
        await pEvent(socket, 'close')
      })
    )
  }

  /**
   * Stop trying to (re)connect to a peer. Hyperswarm keeps redialling a peer
   * passed to `connectPeer` until this is called.
   * @param {string} swarmPublicKey
   */
  leavePeer(swarmPublicKey) {
    this.#swarm?.leavePeer(Buffer.from(swarmPublicKey, 'hex'))
  }

  /**
   * Connect to another peer by their swarm (NOISE) public key. Resolves once
   * the peer has proven its identity. The connection is not yet admitted: use
   * `redeem()` and `waitForAdmission()` to get it admitted via an invite link.
   *
   * @param {string} publicKey
   * @param {object} [opts]
   * @param {number} [opts.timeout]
   * @param {AbortSignal} [opts.signal]
   * @returns {Promise<RemoteAuthedNoiseStream>}
   */
  async connectPeer(publicKey, { timeout = 60_000, signal } = {}) {
    await this.#sm.start()
    const swarm = this.#swarm
    if (!swarm) throw new SwarmNotInitializedError()
    const noisePublicKey = Buffer.from(publicKey, 'hex')

    const existing = await this.#findExistingPeer(noisePublicKey)
    if (existing) return existing

    const onAbort = () => swarm.leavePeer(noisePublicKey)
    const onAuthenticated = pEvent(this, 'authenticated', {
      filter: (connection) => connection.remotePublicKey.equals(noisePublicKey),
      timeout,
      signal,
      // `error` is emitted for any connection's failed handshake, which is
      // unrelated to the one we are waiting for
      rejectionEvents: [],
    })

    swarm.joinPeer(noisePublicKey)
    this.#l.log('Connecting to %S', publicKey)
    signal?.addEventListener('abort', onAbort, { once: true })
    try {
      return await onAuthenticated
    } catch (e) {
      swarm.leavePeer(noisePublicKey)
      if (e instanceof EventTimeoutError) {
        throw new TimeoutError('Timed out waiting for peer')
      }
      throw e
    } finally {
      signal?.removeEventListener('abort', onAbort)
    }
  }

  /**
   * Ask the peer on this connection to redeem an invite link. Resolves once the
   * peer has acknowledged the request (it is now waiting for its user to
   * decide). Rejects if the request is not acknowledged in time or the
   * connection closes. Repeating a redeem for the same invite while it is
   * being decided is allowed (it is the same request); asking for a different
   * invite while one is still being decided is not.
   *
   * @param {RemoteAuthedNoiseStream} socket connection returned by `connectPeer`
   * @param {Redeem} redeem
   * @returns {Promise<void>}
   */
  async redeem(socket, redeem) {
    const conn = this.#getAuthenticatedConnection(socket)
    const request = conn.outgoingRequest
    if (request && !request.settled) {
      if (!timingSafeEqual(request.inviteId, redeem.inviteId)) {
        throw new InviteLinkRequestPendingError()
      }
      // Same invite: a retry of the pending request, keep its decision
    } else {
      conn.startRequest(redeem.inviteId)
    }
    try {
      await conn.inviteLink.sendRedeem(redeem)
    } catch (e) {
      conn.settleRequest(ensureKnownError(e))
      throw e
    }
    // The invitor has the request; from here on we wait on a human, not a timer
    conn.disarmAdmissionTimer()
  }

  /**
   * Wait for the peer on this connection to admit or deny the invite we asked
   * it to redeem. Resolves once admitted (the `connection` event has fired by
   * then). Rejects with UnknownInviteIDError or InviteDeniedByInviterError on
   * a deny, with InviteRedeemConnectionClosedError if the connection closes
   * first, or with the signal's reason if aborted.
   *
   * @param {RemoteAuthedNoiseStream} socket
   * @param {Buffer} inviteId the invite passed to `redeem()` on this connection
   * @param {object} [opts]
   * @param {AbortSignal} [opts.signal]
   * @returns {Promise<void>}
   */
  async waitForAdmission(socket, inviteId, { signal } = {}) {
    const request = this.#getAuthenticatedConnection(socket).outgoingRequest
    if (!request || !timingSafeEqual(request.inviteId, inviteId)) {
      throw new InviteLinkNotRequestedError()
    }
    return abortable(request.decision.promise, signal)
  }

  /**
   * Admit a device that has asked to redeem an invite link. Sends `Admit` on
   * every connection from that device that asked for this invite, and emits
   * `connection` for each that was not already admitted.
   *
   * @param {string} deviceId identity public key as hex
   * @param {Buffer} inviteId
   * @returns {Promise<boolean>} false if no connection from the device is waiting on this invite
   */
  async admit(deviceId, inviteId) {
    let delivered = false
    for (const conn of this.#connectionsAskingFor(deviceId, inviteId)) {
      // Write Admit, then admit on our side in the same tick (see class comment)
      if (!conn.inviteLink?.trySendAdmit({ inviteId })) continue
      this.#markAdmitted(conn)
      delivered = true
    }
    return delivered
  }

  /**
   * Deny a device that has asked to redeem an invite link. Sends `Deny`, waits
   * for the peer to acknowledge it (or the ack timeout), then closes the
   * connection unless the peer is an admitted member, who keeps it.
   *
   * @param {string} deviceId identity public key as hex
   * @param {Buffer} inviteId
   * @param {Deny['reason']} reason
   * @returns {Promise<void>}
   */
  async deny(deviceId, inviteId, reason) {
    for (const conn of this.#connectionsAskingFor(deviceId, inviteId)) {
      conn.incomingInviteId = null
      try {
        await conn.inviteLink?.sendDeny({ inviteId, reason })
      } catch (e) {
        this.#l.log('Failed to send deny to %S: %s', deviceId, e)
      }
      if (conn.state !== 'admitted') conn.socket.end()
    }
  }

  /**
   * @param {string} deviceId
   * @returns {RemoteConnection[]}
   */
  #findByIdentity(deviceId) {
    const result = []
    for (const conn of this.#connections.values()) {
      if (conn.identityHex === deviceId && conn.state !== 'closed') {
        result.push(conn)
      }
    }
    return result
  }

  /**
   * Connections from a device whose latest redeem was for this invite
   * @param {string} deviceId
   * @param {Buffer} inviteId
   */
  #connectionsAskingFor(deviceId, inviteId) {
    return this.#findByIdentity(deviceId).filter(
      ({ incomingInviteId }) =>
        incomingInviteId !== null && timingSafeEqual(incomingInviteId, inviteId)
    )
  }

  /**
   * @param {OpenedNoiseStream} socket
   * @returns {RemoteConnection & { inviteLink: InviteLinkChannel }}
   */
  #getAuthenticatedConnection(socket) {
    const conn = this.#connections.get(socket)
    if (!conn || conn.state === 'handshaking' || !conn.inviteLink) {
      throw new UnknownPeerError({
        deviceId: socket.remotePublicKey?.toString('hex').slice(0, 7),
      })
    }
    if (conn.state === 'closed') throw new InviteRedeemConnectionClosedError()
    return /** @type {RemoteConnection & { inviteLink: InviteLinkChannel }} */ (
      conn
    )
  }

  /**
   * @param {Buffer} noisePublicKey
   * @returns {Promise<RemoteAuthedNoiseStream | null >}
   */
  async #findExistingPeer(noisePublicKey) {
    for (const conn of this.#connections.values()) {
      if (!conn.socket.remotePublicKey?.equals(noisePublicKey)) continue
      try {
        await conn.authenticated.promise
      } catch {
        continue
      }
      if (conn.socket.destroyed || conn.state === 'closed') continue
      return /** @type {RemoteAuthedNoiseStream} */ (conn.socket)
    }
    return null
  }

  /**
   * @param {string} deviceId
   * @returns {boolean}
   */
  #isRecentlyAdmitted(deviceId) {
    const until = this.#admittedDevices.get(deviceId)
    if (until === undefined) return false
    if (until > Date.now()) return true
    this.#admittedDevices.delete(deviceId)
    return false
  }

  /**
   * Called synchronously from the identity-proof message handler once the
   * peer's proof has been verified (see the class comment).
   *
   * @param {RemoteConnection} conn
   * @param {Buffer} identityPublicKey
   */
  #onAuthenticated(conn, identityPublicKey) {
    if (conn.state !== 'handshaking') return
    const socket = conn.socket
    // @ts-expect-error adding AuthedNoiseStream properties
    socket.authenticatedPublicKey = identityPublicKey
    conn.identityPublicKey = identityPublicKey
    conn.state = 'authenticated'
    const deviceId = identityPublicKey.toString('hex')

    conn.authenticated.resolve()
    this.emit('authenticated', /** @type {RemoteAuthedNoiseStream} */ (socket))

    if (this.#isRecentlyAdmitted(deviceId)) {
      // If only we remember the admission (the peer restarted mid-flow), our
      // early RPC open is rejected by the peer and our RPC channel closes,
      // which is harmless: the peer has nothing to resume, and its own
      // admission timeout closes the connection shortly after.
      this.#l.log('Re-admitting recently admitted peer %S', deviceId)
      this.#markAdmitted(conn)
      return
    }
    conn.armAdmissionTimer(this.#admissionTimeout, () => {
      this.#l.log('Peer %S did not redeem an invite in time', deviceId)
      socket.end()
    })
  }

  /** @param {RemoteConnection} conn */
  #markAdmitted(conn) {
    if (conn.state !== 'authenticated') return
    conn.state = 'admitted'
    conn.disarmAdmissionTimer()
    const deviceId = conn.identityHex
    if (deviceId) this.#admittedDevices.set(deviceId, Date.now() + ADMITTED_TTL)
    this.#l.log('Admitted peer %S', deviceId)
    // Consumers set up their channels in this event, synchronously
    this.emit(
      'connection',
      /** @type {RemoteAuthedNoiseStream} */ (conn.socket)
    )
  }

  /**
   * @param {RemoteConnection} conn
   * @param {Error} reason
   */
  #closeConnection(conn, reason) {
    this.#connections.delete(conn.socket)
    const deviceId = conn.identityHex
    if (!conn.close(reason)) return
    // Only the last connection to a device counts as the device going away
    if (deviceId && this.#findByIdentity(deviceId).length === 0) {
      this.emit('peer-closed', deviceId)
    }
  }

  /**
   * @param {OpenedNoiseStream} socket
   */
  async #handleHyperswarmConnection(socket) {
    const conn = new RemoteConnection(socket)
    this.#connections.set(socket, conn)
    const onClose = () =>
      this.#closeConnection(conn, new InviteRedeemConnectionClosedError())
    socket.once('close', onClose)
    socket.once('finish', onClose)
    try {
      if (!(await socket.opened) || socket.destroyed) return
      // Store protomux on the stream so LocalPeers can reuse it
      const protomux = Protomux.from(socket)
      socket.userData = protomux
      // Both channels the peer may open before admission must exist before
      // its first message can arrive (see class comment)
      this.#attachInviteLink(conn, protomux)
      await identityHandshake(
        socket,
        protomux,
        this.#identityKeypair,
        this.#l,
        (identityPublicKey) => this.#onAuthenticated(conn, identityPublicKey)
      )
    } catch (err) {
      const error = ensureKnownError(err)
      this.#closeConnection(conn, error)
      socket.end()
      this.emit('error', error)
    }
  }

  /**
   * @param {RemoteConnection} conn
   * @param {Protomux<any>} protomux
   */
  #attachInviteLink(conn, protomux) {
    const inviteLink = new InviteLinkChannel(protomux, {
      logger: this.#l,
      // Nothing a peer says means anything until it has proven its identity
      acceptRedeem: () =>
        conn.state === 'authenticated' || conn.state === 'admitted',
    })
    inviteLink.on('redeem', (redeem) => this.#onPeerRedeem(conn, redeem))
    inviteLink.on('admit', ({ inviteId }) => this.#onPeerAdmit(conn, inviteId))
    inviteLink.on('deny', (deny) => this.#onPeerDeny(conn, deny))
    inviteLink.on('close', () =>
      conn.settleRequest(new InviteRedeemConnectionClosedError())
    )
    conn.inviteLink = inviteLink
  }

  /**
   * @param {RemoteConnection} conn
   * @param {Redeem} redeem
   */
  #onPeerRedeem(conn, redeem) {
    // A redeem is a request to join a project, which stands whether or not
    // this connection is already admitted (a recently admitted device
    // retrying after a failed join, or a member asking for another link)
    conn.incomingInviteId = redeem.inviteId
    // The invitor's user now has to decide, which can take a while
    conn.disarmAdmissionTimer()
    this.emit('redeem', /** @type {string} */ (conn.identityHex), redeem)
  }

  /**
   * @param {RemoteConnection} conn
   * @param {Buffer} inviteId
   */
  #onPeerAdmit(conn, inviteId) {
    if (!conn.hasPendingRequestFor(inviteId)) {
      // Only a redeem we sent can lead to admission, so an unasked-for Admit
      // comes from a peer that is not following the protocol
      this.#l.log('Unsolicited admit from %S, disconnecting', conn.identityHex)
      conn.socket.end()
      return
    }
    this.#markAdmitted(conn)
    conn.settleRequest()
  }

  /**
   * @param {RemoteConnection} conn
   * @param {Deny} deny
   */
  #onPeerDeny(conn, deny) {
    if (!conn.hasPendingRequestFor(deny.inviteId)) {
      this.#l.log('Ignoring deny from %S: not requested', conn.identityHex)
      return
    }
    this.#l.log('Denied by %S: %s', conn.identityHex, deny.reason)
    const error =
      deny.reason === 'unknown_invite_id'
        ? new UnknownInviteIDError()
        : new InviteDeniedByInviterError({ reason: deny.reason })
    conn.settleRequest(error)
  }
}
