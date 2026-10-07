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
import { PROTOCOL_NAME as RPC_PROTOCOL_NAME } from '../local-peers.js'
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
 * @property {(deviceId: string, redeem: Redeem) => void} redeem Emitted when an authenticated peer asks to redeem an invite link. The request has already been acknowledged; call `admit()` or `deny()` to decide.
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
 * redeem.
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
  /** @type {DeferredPromise<void>} resolves when admitted, rejects on deny or close */
  admitted = pDefer()

  /** @param {OpenedNoiseStream} socket */
  constructor(socket) {
    this.socket = socket
    // These may legitimately never be awaited
    this.authenticated.promise.catch(noop)
    this.admitted.promise.catch(noop)
  }

  get identityHex() {
    return this.identityPublicKey?.toString('hex')
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
   * connection closes.
   *
   * @param {RemoteAuthedNoiseStream} socket connection returned by `connectPeer`
   * @param {Redeem} redeem
   * @returns {Promise<void>}
   */
  async redeem(socket, redeem) {
    const conn = this.#getAuthenticatedConnection(socket)
    await conn.inviteLink.sendRedeem(redeem)
    // The invitor has the request; from here on we wait on a human, not a timer
    conn.awaitingDecision = true
    this.#clearAdmissionTimer(conn)
  }

  /**
   * Wait for the peer on this connection to admit or deny us. Resolves once
   * admitted (the `connection` event has fired by then). Rejects with
   * UnknownInviteIDError or InviteDeniedByInviterError on a deny, with
   * InviteRedeemConnectionClosedError if the connection closes first, or with
   * the signal's reason if aborted.
   *
   * @param {RemoteAuthedNoiseStream} socket
   * @param {object} [opts]
   * @param {AbortSignal} [opts.signal]
   * @returns {Promise<void>}
   */
  async waitForAdmission(socket, { signal } = {}) {
    const conn = this.#getAuthenticatedConnection(socket)
    if (!signal) return conn.admitted.promise
    signal.throwIfAborted()
    /** @type {() => void} */
    let onAbort = noop
    const abortPromise = new Promise((_, reject) => {
      onAbort = () => reject(signal.reason)
      signal.addEventListener('abort', onAbort, { once: true })
    })
    try {
      await Promise.race([conn.admitted.promise, abortPromise])
    } finally {
      signal.removeEventListener('abort', onAbort)
    }
  }

  /**
   * Admit a device that has redeemed an invite link. Sends `Admit` on every
   * pending connection from that device and emits `connection` for each.
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
      // our side (a recently admitted device reconnecting): the peer may not
      // have admitted us, and Admit is idempotent on the receiver
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

  /** @param {RemoteConnection} conn */
  #admit(conn) {
    if (conn.state !== 'authenticated') return
    conn.state = 'admitted'
    this.#clearAdmissionTimer(conn)
    const deviceId = conn.identityHex
    if (deviceId) this.#admittedDevices.set(deviceId, Date.now() + ADMITTED_TTL)
    this.#l.log('Admitted peer %S', deviceId)
    // Emit before resolving so that consumers set up their channels (LocalPeers
    // pairs the RPC protocol synchronously) before anything awaiting admission
    // continues
    this.emit(
      'connection',
      /** @type {RemoteAuthedNoiseStream} */ (conn.socket)
    )
    conn.admitted.resolve()
  }

  /**
   * @param {RemoteConnection} conn
   * @param {Error} reason
   */
  #closeConnection(conn, reason) {
    this.#connections.delete(conn.socket)
    this.#clearAdmissionTimer(conn)
    if (conn.state === 'closed') return
    const wasAdmitted = conn.state === 'admitted'
    const deviceId = conn.identityHex
    conn.state = 'closed'
    conn.authenticated.reject(reason)
    if (!wasAdmitted) conn.admitted.reject(reason)
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

      // Protomux rejects an incoming channel open for a protocol that has
      // neither a channel nor a pairing yet, so everything the peer may open
      // must be ready (or paired) before the peer can send it:
      //
      // - The invite-link channel is created now, alongside the auth channel.
      //   Its messages are ignored until the peer has proven its identity.
      // - The RPC channel is only created by LocalPeers once admitted, so hold
      //   any early open from the peer until then (the peer admits us first and
      //   may open RPC before our own admission has been processed).
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
      inviteLink.on('admit', () => this.#admit(conn))
      inviteLink.on('deny', (deny) => {
        if (conn.state !== 'authenticated') return
        this.#l.log('Denied by %S: %s', conn.identityHex, deny.reason)
        conn.admitted.reject(denyToError(deny))
      })
      inviteLink.on('close', () => {
        if (conn.state !== 'authenticated') return
        // Peer closed (or rejected) the invite-link channel without a decision
        conn.admitted.reject(new InviteRedeemConnectionClosedError())
      })
      protomux.pair({ protocol: RPC_PROTOCOL_NAME }, async () => {
        try {
          await conn.admitted.promise
        } catch {
          return // not admitted: protomux rejects the peer's open
        }
        // Let the consumer of the `connection` event create its channel before
        // protomux decides whether the held open was answered
        await new Promise((resolve) => setImmediate(resolve))
      })

      const identityPublicKey = await identityHandshake(
        socket,
        protomux,
        this.#identityKeypair,
        this.#l
      )
      if (conn.state === 'closed') return

      // @ts-expect-error adding AuthedNoiseStream properties
      socket.authenticatedPublicKey = identityPublicKey
      conn.identityPublicKey = identityPublicKey
      conn.state = 'authenticated'
      const deviceId = identityPublicKey.toString('hex')

      conn.authenticated.resolve()
      this.emit(
        'authenticated',
        /** @type {RemoteAuthedNoiseStream} */ (socket)
      )

      if (this.#isRecentlyAdmitted(deviceId)) {
        this.#l.log('Re-admitting recently admitted peer %S', deviceId)
        this.#admit(conn)
        return
      }
      this.#startAdmissionTimer(conn)
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
 * Hello messages, then exchange identity proofs. Resolves with the peer's
 * verified identity public key.
 *
 * @param {OpenedNoiseStream} socket
 * @param {Protomux<any>} protomux
 * @param {Keypair} identityKeypair
 * @param {Logger} logger
 * @returns {Promise<Buffer>}
 */
async function identityHandshake(socket, protomux, identityKeypair, logger) {
  const remotePublicKeyString = socket.remotePublicKey.toString('hex')

  const helloDefer = pDefer()
  const identityDefer = pDefer()
  const onAuthOpen = pDefer()
  /** @type {ReturnType<typeof pDefer>} */
  let drainDefer

  const messages = [
    {
      encoding: cenc.raw,
      onmessage: /** @param {Buffer} msg */ (msg) => {
        const hello = Hello.decode(msg)
        if (hello.protocolVersion !== AUTH_PROTOCOL_VERSION) {
          logger.log(
            'Peer %s has incompatible protocol version %d',
            remotePublicKeyString,
            hello.protocolVersion
          )
          helloDefer.reject(new AuthProtocolVersionMismatchError())
          return
        }
        helloDefer.resolve(hello)
      },
    },
    {
      encoding: cenc.raw,
      onmessage: /** @param {Buffer} msg */ (msg) => {
        identityDefer.resolve(IdentityProof.decode(msg))
      },
    },
  ]

  const authChannel = protomux.createChannel({
    protocol: AUTH_PROTOCOL,
    messages,
    onopen: () => onAuthOpen.resolve(),
    ondrain: () => drainDefer?.resolve(),
  })
  if (!authChannel) throw new InviteRedeemConnectionClosedError()
  authChannel.open()
  await onAuthOpen.promise

  /**
   * @param {Buffer} buf
   * @param {number} messageId
   */
  const sendAndDrain = async (buf, messageId) => {
    drainDefer = pDefer()
    const didWrite = authChannel.messages[messageId].send(buf)
    if (!didWrite) await drainDefer.promise
  }

  // Send our hello
  const myHello = Hello.encode({
    protocolVersion: AUTH_PROTOCOL_VERSION,
  }).finish()
  await sendAndDrain(Buffer.from(myHello), 0)

  // Receive peer's hello
  await timeoutPromise(helloDefer.promise, {
    milliseconds: AUTH_HANDSHAKE_TIMEOUT,
  })

  // Send our identity proof
  const sig = new Uint8Array(64)
  sodium.crypto_sign_detached(
    sig,
    socket.handshakeHash,
    identityKeypair.secretKey
  )
  const myProof = IdentityProof.encode({
    publicKey: identityKeypair.publicKey,
    signature: Buffer.from(sig),
  }).finish()
  await sendAndDrain(Buffer.from(myProof), 1)

  // Receive and verify peer's identity proof
  const peerProof = await timeoutPromise(identityDefer.promise, {
    milliseconds: AUTH_HANDSHAKE_TIMEOUT,
  })

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
  if (!valid) throw new InvalidIdentityProofError()

  return Buffer.from(peerProof.publicKey)
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
