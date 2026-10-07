import sodium from 'sodium-universal'
import cenc from 'compact-encoding'
import pDefer from 'p-defer'
import { timeoutPromise } from '../utils.js'
import { Hello, IdentityProof } from '../generated/auth.js'
import {
  AuthProtocolVersionMismatchError,
  InvalidIdentityProofError,
  InviteRedeemConnectionClosedError,
} from '../errors.js'

/** @import Protomux from 'protomux' */
/** @import { OpenedNoiseStream } from '../lib/noise-secret-stream-helpers.js' */
/** @import { Keypair } from './local-discovery.js' */
/** @import { Logger } from '../logger.js' */
/** @import { DeferredPromise } from 'p-defer' */

export const AUTH_PROTOCOL = 'comapeo/auth'
export const AUTH_PROTOCOL_VERSION = 1
export const AUTH_HANDSHAKE_TIMEOUT = 10_000

/**
 * Run the `comapeo/auth` handshake on a freshly opened NOISE stream: exchange
 * Hello messages, then exchange identity proofs, so that each side learns the
 * stable identity key behind the peer's ephemeral swarm key.
 *
 * The handshake is driven by the message handlers rather than by sequential
 * awaits: our proof is sent inside the Hello handler and `onAuthenticated` is
 * called inside the proof handler, so that each step completes before protomux
 * moves on to the peer's next message (protomux processes a whole received
 * chunk synchronously). The returned promise only reports the outcome and
 * enforces the timeout; it resolves after `onAuthenticated` has run.
 *
 * @param {OpenedNoiseStream} socket
 * @param {Protomux<any>} protomux
 * @param {Keypair} identityKeypair
 * @param {Logger} logger
 * @param {(identityPublicKey: Buffer) => void} onAuthenticated
 * @returns {Promise<void>}
 */
export function identityHandshake(
  socket,
  protomux,
  identityKeypair,
  logger,
  onAuthenticated
) {
  /** @type {DeferredPromise<void>} */
  const done = pDefer()
  let gotHello = false

  /** @param {Buffer} msg */
  const onHello = (msg) => {
    if (gotHello) return // duplicate Hello, ignore
    const hello = Hello.decode(msg)
    if (hello.protocolVersion !== AUTH_PROTOCOL_VERSION) {
      logger.log(
        'Peer %h has incompatible protocol version %d',
        socket.remotePublicKey,
        hello.protocolVersion
      )
      done.reject(new AuthProtocolVersionMismatchError())
      return
    }
    gotHello = true
    // Versions agree: prove our identity, synchronously, so that our proof
    // precedes anything we write after authenticating
    const signature = new Uint8Array(64)
    sodium.crypto_sign_detached(
      signature,
      socket.handshakeHash,
      identityKeypair.secretKey
    )
    const proof = IdentityProof.encode({
      publicKey: identityKeypair.publicKey,
      signature: Buffer.from(signature),
    }).finish()
    authChannel.messages[1].send(Buffer.from(proof))
  }

  /** @param {Buffer} msg */
  const onProof = (msg) => {
    // Proof before Hello: not a peer speaking this protocol
    if (!gotHello) return done.reject(new InvalidIdentityProofError())
    const proof = IdentityProof.decode(msg)
    if (!verifyProof(proof, socket.handshakeHash)) {
      return done.reject(new InvalidIdentityProofError())
    }
    try {
      onAuthenticated(Buffer.from(proof.publicKey))
    } catch (e) {
      return done.reject(e)
    }
    done.resolve()
  }

  const authChannel = protomux.createChannel({
    protocol: AUTH_PROTOCOL,
    messages: [
      { encoding: cenc.raw, onmessage: onHello },
      { encoding: cenc.raw, onmessage: onProof },
    ],
    onopen: () => {
      const hello = Hello.encode({
        protocolVersion: AUTH_PROTOCOL_VERSION,
      }).finish()
      authChannel.messages[0].send(Buffer.from(hello))
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
 * @param {IdentityProof} proof
 * @param {Buffer | null} handshakeHash null only before the NOISE handshake, which has completed by the time any message arrives
 * @returns {boolean}
 */
function verifyProof(proof, handshakeHash) {
  if (!handshakeHash) return false
  try {
    return sodium.crypto_sign_verify_detached(
      proof.signature,
      handshakeHash,
      proof.publicKey
    )
  } catch {
    return false
  }
}
