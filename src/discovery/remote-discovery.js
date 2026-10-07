import { TypedEmitter } from 'tiny-typed-emitter'
import { Logger } from '../logger.js'
import Hyperswarm from 'hyperswarm'
import StartStopStateMachine from 'start-stop-state-machine'
import { pEvent, TimeoutError as EventTimeoutError } from 'p-event'
import sodium from 'sodium-universal'
import Protomux from 'protomux'
import cenc from 'compact-encoding'
import pDefer from 'p-defer'
import { noop, timeoutPromise } from '../utils.js'
import { Hello, IdentityProof } from '../generated/auth.js'
import { InviteLinkChannel } from '../invite/invite-link-channel.js'
import {
  AuthProtocolVersionMismatchError,
  ensureKnownError,
  InvalidIdentityProofError,
  InviteDeniedByInviterError,
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
 * @property {(deviceId: string) => void} peer-closed Emitted when a connection to an authenticated peer closes, whether or not it was admitted
 * @property {(error: Error) => void} error
 */

// Symbol for test-only access to internal methods
export const kTestOnlyHandleHyperswarmConnection = Symbol(
  'testOnlyHandleHyperswarmConnection'
)

export const AUTH_PROTOCOL = 'comapeo/auth'
const AUTH_PROTOCOL_VERSION = 1
const AUTH_HANDSHAKE_TIMEOUT = 10_000

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
 * Per-connection state owned by RemoteDiscovery. A connection climbs
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
  /** @type {ReturnType<typeof setTimeout> | null} */
  admissionTimer = null
  /** True once the peer has sent us a redeem, or we have had ours acked */
  awaitingDecision = false
  /** @type {DeferredPromise<void>} resolves when authenticated, rejects on failure */
  authenticated = pDefer()
  /**
   * The invitor's decision on each invite link we have redeemed on this
   * connection, keyed by invite ID hex. Resolves on Admit for that invite,
   * rejects on Deny for that invite or when the connection closes. Kept per
   * invite so that two links redeemed on one connection are decided
   * independently.
   * @type {Map<string, DeferredPromise<void>>}
   */
  decisions = new Map()

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
   * @param {Buffer} inviteId
   * @returns {DeferredPromise<void>}
   */
  decision(inviteId) {
    const key = inviteId.toString('hex')
    let deferred = this.decisions.get(key)
    if (!deferred) {
      deferred = pDefer()
      deferred.promise.catch(noop)
      this.decisions.set(key, deferred)
    }
    return deferred
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
 *    this device recently and it has reconnected.
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
 * be processed.
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
      this.#clearAdmissionTimer(conn)
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
   * Disconnect from a peer by their swarm (NOISE) public key or their identity
   * public key.
   * @param {string} publicKey
   */
  async disconnectPeer(publicKey) {
    const key = Buffer.from(publicKey, 'hex')

    for (const conn of this.#connections.values()) {
      if (
        conn.socket.remotePublicKey?.equals(key) ||
        conn.identityPublicKey?.equals(key)
      ) {
        this.#l.log('Disconnecting from peer %S', publicKey)
        conn.socket.end()
        await pEvent(conn.socket, 'close')
        return
      }
    }
    this.#l.log('Cannot disconnect from peer %S, not connected', publicKey)
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

    const onAbort = () => {
      this.#l.log('Leave peer for %s', publicKey)
      swarm.leavePeer(noisePublicKey)
    }

    const onAuthenticated = pEvent(this, 'authenticated', {
      filter: (connection) => connection.remotePublicKey.equals(noisePublicKey),
      timeout,
      signal,
    })

    // Start trying to connect
    swarm.joinPeer(noisePublicKey)
    this.#l.log('Connecting to %S', publicKey)
    signal?.addEventListener('abort', onAbort, { once: true })
    try {
      return await onAuthenticated
    } catch (e) {
      // We should stop trying to connect if we time out
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
   * connection closes. Repeating a redeem for the same invite is allowed and
   * is treated by the peer as the same request.
   *
   * @param {RemoteAuthedNoiseStream} socket connection returned by `connectPeer`
   * @param {Redeem} redeem
   * @returns {Promise<void>}
   */
  async redeem(socket, redeem) {
    const conn = this.#getAuthenticatedConnection(socket)
    // Register interest in the decision before the peer can possibly answer
    conn.decision(redeem.inviteId)
    await conn.inviteLink.sendRedeem(redeem)
    // The invitor has the request; from here on we wait on a human, not a timer
    conn.awaitingDecision = true
    this.#clearAdmissionTimer(conn)
  }

  /**
   * Wait for the peer on this connection to admit or deny the given invite.
   * Resolves once admitted (the `connection` event has fired by then). Rejects
   * with UnknownInviteIDError or InviteDeniedByInviterError on a deny of this
   * invite, with InviteRedeemConnectionClosedError if the connection closes
   * first, or with the signal's reason if aborted. A decision on a different
   * invite redeemed on the same connection does not affect this wait.
   *
   * @param {RemoteAuthedNoiseStream} socket
   * @param {Buffer} inviteId
   * @param {object} [opts]
   * @param {AbortSignal} [opts.signal]
   * @returns {Promise<void>}
   */
  async waitForAdmission(socket, inviteId, { signal } = {}) {
    const conn = this.#getAuthenticatedConnection(socket)
    const decision = conn.decision(inviteId).promise
    if (!signal) return decision
    signal.throwIfAborted()
    /** @type {() => void} */
    let onAbort = noop
    const abortPromise = new Promise((_, reject) => {
      onAbort = () => reject(signal.reason)
      signal.addEventListener('abort', onAbort, { once: true })
    })
    try {
      await Promise.race([decision, abortPromise])
    } finally {
      signal.removeEventListener('abort', onAbort)
    }
  }

  /**
   * Admit a device that has redeemed an invite link. Sends `Admit` on every
   * connection from that device and emits `connection` for each that was not
   * already admitted.
   *
   * @param {string} deviceId identity public key as hex
   * @param {Buffer} inviteId
   * @returns {Promise<boolean>} false if the device is not connected
   */
  async admit(deviceId, inviteId) {
    let found = false
    for (const conn of this.#findByIdentity(deviceId)) {
      found = true
      // Always tell the peer, even if we already admitted this connection on
      // our side (a recently admitted device reconnecting): the peer is
      // waiting for the decision on this invite, and Admit is idempotent for
      // connection-level admission on the receiver
      try {
        await conn.inviteLink?.sendAdmit({ inviteId })
      } catch (e) {
        this.#l.log('Failed to send admit to %S: %s', deviceId, e)
        continue
      }
      this.#admit(conn)
    }
    return found
  }

  /**
   * Deny a device that has redeemed an invite link. Sends `Deny`, waits for the
   * peer to acknowledge it (or the ack timeout), then closes the connection.
   *
   * @param {string} deviceId identity public key as hex
   * @param {Buffer} inviteId
   * @param {Deny['reason']} reason
   * @returns {Promise<void>}
   */
  async deny(deviceId, inviteId, reason) {
    for (const conn of this.#findByIdentity(deviceId)) {
      try {
        await conn.inviteLink?.sendDeny({ inviteId, reason })
      } catch (e) {
        this.#l.log('Failed to send deny to %S: %s', deviceId, e)
      }
      conn.socket.end()
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

  /** @param {RemoteConnection} conn */
  #clearAdmissionTimer(conn) {
    if (conn.admissionTimer === null) return
    clearTimeout(conn.admissionTimer)
    conn.admissionTimer = null
  }

  /** @param {RemoteConnection} conn */
  #startAdmissionTimer(conn) {
    this.#clearAdmissionTimer(conn)
    conn.admissionTimer = setTimeout(() => {
      conn.admissionTimer = null
      if (conn.state === 'admitted' || conn.awaitingDecision) return
      this.#l.log(
        'Peer %S was not admitted within %d ms, disconnecting',
        conn.identityHex,
        this.#admissionTimeout
      )
      conn.socket.end()
    }, this.#admissionTimeout)
    conn.admissionTimer.unref()
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
   * peer's proof has been verified (see the class comment for why this must
   * not run in an `await` continuation).
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
      this.#admit(conn)
      return
    }
    this.#startAdmissionTimer(conn)
  }

  /** @param {RemoteConnection} conn */
  #admit(conn) {
    if (conn.state !== 'authenticated') return
    conn.state = 'admitted'
    this.#clearAdmissionTimer(conn)
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
    this.#clearAdmissionTimer(conn)
    if (conn.state === 'closed') return
    const deviceId = conn.identityHex
    conn.state = 'closed'
    conn.authenticated.reject(reason)
    for (const decision of conn.decisions.values()) decision.reject(reason)
    conn.inviteLink?.close()
    if (deviceId) this.emit('peer-closed', deviceId)
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
      // Wait for the NOISE handshake to complete
      const opened = await socket.opened
      if (!opened || socket.destroyed) return

      // Create protomux and store on the stream so LocalPeers can reuse it
      const protomux = Protomux.from(socket)
      socket.userData = protomux

      // The invite-link channel is created now, alongside the auth channel,
      // because the peer may open it as soon as its own handshake completes.
      // Its messages are ignored until the peer has proven its identity.
      const inviteLink = new InviteLinkChannel(protomux, { logger: this.#l })
      conn.inviteLink = inviteLink
      inviteLink.on('redeem', (redeem) => {
        // A redeem is a request to join a project, which stands whether or not
        // this connection is already admitted (e.g. a recently admitted device
        // retrying after a failed join)
        if (conn.state !== 'authenticated' && conn.state !== 'admitted') {
          this.#l.log('Ignoring redeem from peer in state %s', conn.state)
          return
        }
        // The invitor's user now has to decide, which can take a while
        conn.awaitingDecision = true
        this.#clearAdmissionTimer(conn)
        this.emit('redeem', /** @type {string} */ (conn.identityHex), redeem)
      })
      inviteLink.on('admit', ({ inviteId }) => {
        // Connection-level admission on any Admit, decision per invite
        this.#admit(conn)
        conn.decision(inviteId).resolve()
      })
      inviteLink.on('deny', (deny) => {
        this.#l.log('Denied by %S: %s', conn.identityHex, deny.reason)
        conn.decision(deny.inviteId).reject(denyToError(deny))
      })
      inviteLink.on('close', () => {
        // Peer closed (or rejected) the invite-link channel: no decision on
        // anything redeemed over it can arrive any more
        for (const decision of conn.decisions.values()) {
          decision.reject(new InviteRedeemConnectionClosedError())
        }
      })

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
}

/**
 * Run the `comapeo/auth` handshake on a freshly opened NOISE stream: exchange
 * Hello messages, then exchange identity proofs.
 *
 * The handshake is driven by the message handlers rather than by sequential
 * awaits: our proof is sent inside the Hello handler and `onAuthenticated` is
 * called inside the proof handler, so that each step completes before protomux
 * moves on to the peer's next message. The returned promise only reports the
 * outcome (and enforces the timeout); it resolves after `onAuthenticated` has
 * run.
 *
 * @param {OpenedNoiseStream} socket
 * @param {Protomux<any>} protomux
 * @param {Keypair} identityKeypair
 * @param {Logger} logger
 * @param {(identityPublicKey: Buffer) => void} onAuthenticated
 * @returns {Promise<void>}
 */
function identityHandshake(
  socket,
  protomux,
  identityKeypair,
  logger,
  onAuthenticated
) {
  const remotePublicKeyString = socket.remotePublicKey.toString('hex')
  /** @type {DeferredPromise<void>} */
  const done = pDefer()
  let gotHello = false

  const messages = [
    {
      encoding: cenc.raw,
      onmessage: /** @param {Buffer} msg */ (msg) => {
        if (gotHello) return // duplicate Hello, ignore
        const hello = Hello.decode(msg)
        if (hello.protocolVersion !== AUTH_PROTOCOL_VERSION) {
          logger.log(
            'Peer %s has incompatible protocol version %d',
            remotePublicKeyString,
            hello.protocolVersion
          )
          done.reject(new AuthProtocolVersionMismatchError())
          return
        }
        gotHello = true
        // Versions agree: prove our identity. Sent here, synchronously, so
        // that our proof precedes anything we write after authenticating.
        const sig = new Uint8Array(64)
        sodium.crypto_sign_detached(
          sig,
          socket.handshakeHash,
          identityKeypair.secretKey
        )
        const proof = IdentityProof.encode({
          publicKey: identityKeypair.publicKey,
          signature: Buffer.from(sig),
        }).finish()
        authChannel.messages[1].send(Buffer.from(proof))
      },
    },
    {
      encoding: cenc.raw,
      onmessage: /** @param {Buffer} msg */ (msg) => {
        if (!gotHello) {
          // Proof before Hello: not a peer speaking this protocol
          done.reject(new InvalidIdentityProofError())
          return
        }
        const peerProof = IdentityProof.decode(msg)
        let valid
        try {
          valid = sodium.crypto_sign_verify_detached(
            peerProof.signature,
            socket.handshakeHash,
            peerProof.publicKey
          )
        } catch {
          valid = false
        }
        if (!valid) {
          done.reject(new InvalidIdentityProofError())
          return
        }
        try {
          onAuthenticated(Buffer.from(peerProof.publicKey))
        } catch (e) {
          done.reject(e)
          return
        }
        done.resolve()
      },
    },
  ]

  const authChannel = protomux.createChannel({
    protocol: AUTH_PROTOCOL,
    messages,
    onopen: () => {
      const myHello = Hello.encode({
        protocolVersion: AUTH_PROTOCOL_VERSION,
      }).finish()
      authChannel.messages[0].send(Buffer.from(myHello))
    },
    onclose: () => done.reject(new InviteRedeemConnectionClosedError()),
  })
  if (!authChannel) throw new InviteRedeemConnectionClosedError()
  authChannel.open()

  return timeoutPromise(done.promise, {
    milliseconds: AUTH_HANDSHAKE_TIMEOUT,
  })
}

/**
 * @param {Deny} deny
 * @returns {Error}
 */
function denyToError(deny) {
  return deny.reason === 'unknown_invite_id'
    ? new UnknownInviteIDError()
    : new InviteDeniedByInviterError({ reason: deny.reason })
}
