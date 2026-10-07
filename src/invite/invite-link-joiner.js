import { TypedEmitter } from 'tiny-typed-emitter'
import { pEvent } from 'p-event'
import { parseInviteURL } from './invite-urls.js'
import { DeviceInfo_DeviceType } from '../generated/rpc.js'
import { Logger } from '../logger.js'
import { noop, timeoutPromise } from '../utils.js'
import {
  ExistingJoinRequestError,
  InviteRedeemConnectionClosedError,
  InviteConnectionError,
  JoinRequestNotFoundError,
  JoinProjectCancelledError,
  TimeoutError,
  PeerDisconnectedError,
  RPCDisconnectBeforeSendingError,
  RPCDisconnectBeforeAckError,
  UnknownPeerError,
  InitialSyncFailedError,
  ensureKnownError,
} from '../errors.js'

/**
 * How long to wait, once admitted, for the invitor's Invite to arrive over
 * RPC. The invitor sends it as soon as it admits us, so this only covers a
 * slow or stalled invitor; it is not a human-decision wait.
 */
export const INVITE_AFTER_ADMISSION_TIMEOUT_MS = 30_000

/**
 * Error codes that indicate a network/transport failure
 * @type {Set<string>}
 */
const NETWORK_ERROR_CODES = new Set([
  TimeoutError.code,
  InviteRedeemConnectionClosedError.code,
  PeerDisconnectedError.code,
  RPCDisconnectBeforeSendingError.code,
  RPCDisconnectBeforeAckError.code,
  UnknownPeerError.code,
  InitialSyncFailedError.code,
])

/** @import { RemoteDiscovery } from '../discovery/remote-discovery.js' */
/** @import { InviteApi } from '../invite/invite-api.js' */

/**
 * - `connecting`: finding and connecting to the invitor, verifying its identity
 * - `connected`: identity verified, sending the redeem request
 * - `requested`: the invitor has acknowledged the request and its user is deciding
 * - `accepted`: the invitor accepted; receiving the invite and joining
 * - `completed`: joined, `projectId` is set
 * - `failed`: `error` is set
 *
 * @typedef {'connecting' | 'connected' | 'requested' | 'accepted' | 'completed' | 'failed'} JoinRequestStatus
 */

/**
 * A join request initiated via an invite URL. Listen to `join-request-update`
 * events for progress.
 *
 * @typedef {object} JoinRequest
 * @property {string} inviteId Hex invite ID
 * @property {string} swarmPublicKey Invitor's swarm public key
 * @property {string} url Original invite URL
 * @property {JoinRequestStatus} status Current status
 * @property {Error|null} error Error if status is 'failed'
 * @property {string|undefined} projectId Resolved project ID on completion
 */

/**
 * @typedef {object} JoinRequestUpdate
 * @property {JoinRequestStatus} status
 * @property {string} inviteId
 * @property {Error|null} [error]
 * @property {string|undefined} [projectId]
 */

/**
 * @typedef {Pick<RemoteDiscovery, 'connectPeer' | 'redeem' | 'waitForAdmission' | 'disconnectPeer' | 'leavePeer'>} JoinerDiscovery
 */

/**
 * @typedef {object} InviteLinkJoinerOptions
 * @property {JoinerDiscovery} discovery Remote discovery used to connect to and get admitted by the invitor
 * @property {Pick<InviteApi, 'on' | 'accept'>} inviteApi Invite API (on + accept only)
 * @property {() => { name?: string, deviceType?: string }} getDeviceInfo Our device info, shown to the invitor when they decide
 * @property {number} [defaultTimeout] Default timeout in ms for peer connection (default: 60_000)
 * @property {number} [inviteTimeout] ms to wait for the Invite after admission (default: 30_000)
 * @property {Logger} [logger]
 */

/**
 * @typedef {object} InviteLinkJoinerEvents
 * @property {(update: JoinRequestUpdate) => void} join-request-update
 */

/**
 * @typedef {object} PendingJoinRequest
 * @property {AbortController} abortController
 * @property {JoinRequest} joinRequest
 */

/**
 * Invitee side of invites over the internet: turns an invite URL into a
 * project membership by connecting to the invitor, redeeming the invite link
 * on the invite-link channel, and then accepting the regular invite that the
 * invitor sends over RPC once it has admitted us.
 *
 * @extends {TypedEmitter<InviteLinkJoinerEvents>}
 */
export class InviteLinkJoiner extends TypedEmitter {
  #discovery
  #inviteApi
  #getDeviceInfo
  #defaultTimeout
  #inviteTimeout
  #l
  /** @type {Map<string, PendingJoinRequest>} */
  #pending = new Map()

  /**
   * @param {InviteLinkJoinerOptions} options
   */
  constructor({
    discovery,
    inviteApi,
    getDeviceInfo,
    defaultTimeout = 60_000,
    inviteTimeout = INVITE_AFTER_ADMISSION_TIMEOUT_MS,
    logger,
  }) {
    super()
    this.#l = Logger.create('inviteLinkJoiner', logger)
    this.#discovery = discovery
    this.#inviteApi = inviteApi
    this.#getDeviceInfo = getDeviceInfo
    this.#defaultTimeout = defaultTimeout
    this.#inviteTimeout = inviteTimeout
  }

  /**
   * Create and start a join request from an invite URL.
   *
   * @param {string} url Invite URL
   * @param {object} [opts]
   * @param {number} [opts.timeout] Connection timeout in ms
   * @returns {JoinRequest}
   */
  createJoinRequest(url, { timeout = this.#defaultTimeout } = {}) {
    const parsed = parseInviteURL(url)
    const inviteId = parsed.inviteIdString

    if (this.#pending.has(inviteId)) {
      throw new ExistingJoinRequestError({ inviteId })
    }

    const abortController = new AbortController()
    /** @type {JoinRequest} */
    const joinRequest = {
      inviteId,
      swarmPublicKey: parsed.swarmPublicKey,
      url,
      status: 'connecting',
      error: null,
      projectId: undefined,
    }
    this.#pending.set(inviteId, { abortController, joinRequest })

    this.#emitUpdate(joinRequest)

    // Start the async flow (fire-and-forget)
    this.#runJoinFlow(joinRequest, timeout, abortController.signal)

    return joinRequest
  }

  /**
   * @param {JoinRequest} joinRequest
   * @param {number} timeout
   * @param {AbortSignal} signal
   */
  async #runJoinFlow(joinRequest, timeout, signal) {
    const { inviteId: inviteIdString, swarmPublicKey } = joinRequest
    const inviteId = Buffer.from(inviteIdString, 'hex')

    try {
      const connection = await this.#discovery.connectPeer(swarmPublicKey, {
        timeout,
        signal,
      })

      // Connected: the invitor has proven its identity
      this.#setStatus(joinRequest, 'connected')

      // Use the identity key from the handshake, not the swarm key from the URL
      const invitorDeviceId = connection.authenticatedPublicKey.toString('hex')

      // Start listening for the invite before we can be admitted, so a fast
      // invitor cannot send it before we are listening
      const onInvited = pEvent(this.#inviteApi, 'invite-received', {
        filter: (invite) => invite.invitorDeviceId === invitorDeviceId,
        signal,
      })
      onInvited.catch(noop)

      const { name = '', deviceType } = this.#getDeviceInfo()
      await this.#discovery.redeem(connection, {
        inviteId,
        deviceName: name,
        deviceType: toRpcDeviceType(deviceType),
      })

      // Requested: the invitor has our request and its user is deciding
      this.#setStatus(joinRequest, 'requested')

      // Resolves when this invite is admitted, rejects on deny of this
      // invite, disconnect or cancel
      await this.#discovery.waitForAdmission(connection, inviteId, { signal })

      // Accepted: we are admitted, the regular invite follows over RPC
      this.#setStatus(joinRequest, 'accepted')

      const onClose = pEvent(connection, 'close').then(
        () => {
          throw new InviteRedeemConnectionClosedError()
        },
        // Handle `error` event on connection if there's sudden closes
        (e) => {
          throw new InviteRedeemConnectionClosedError({ cause: e })
        }
      )
      onClose.catch(noop)

      // An admitted peer that never receives the Invite (e.g. we were already
      // a member and the invitor's RPC answered "already") must not wait
      // forever
      const invite = await timeoutPromise(Promise.race([onInvited, onClose]), {
        milliseconds: this.#inviteTimeout,
      })

      const projectId = await this.#inviteApi.accept(invite)

      // Completed. The connection is deliberately left open: our initial sync
      // being done does not mean the invitor's is (it still wants our initial
      // data and checks our role before it considers us joined), and we are
      // now a member, so this is an ordinary sync connection. It closes when
      // either side stops its swarm or the app closes.
      joinRequest.projectId = projectId
      this.#setStatus(joinRequest, 'completed')
    } catch (e) {
      joinRequest.error = wrapNetworkError(e)
      this.#setStatus(joinRequest, 'failed')
      this.#l.log('join request %S failed: %s', inviteIdString, e)

      try {
        await this.#discovery.disconnectPeer(swarmPublicKey)
      } catch {
        // ignore disconnect errors
      }
    } finally {
      // Hyperswarm keeps redialling a joined peer until we leave it. Stop the
      // redials now (any open connection stays open); if it drops later the
      // invitor remembers our admission for a while and project discovery
      // over the swarm, when it exists, is the way to reconnect.
      this.#discovery.leavePeer(swarmPublicKey)
      this.#pending.delete(inviteIdString)
    }
  }

  /**
   * @param {JoinRequest} joinRequest
   * @param {JoinRequestStatus} status
   */
  #setStatus(joinRequest, status) {
    joinRequest.status = status
    this.#emitUpdate(joinRequest)
  }

  /**
   * @param {JoinRequest} joinRequest
   */
  #emitUpdate(joinRequest) {
    /** @type {JoinRequestUpdate} */
    const update = {
      status: joinRequest.status,
      inviteId: joinRequest.inviteId,
      error: joinRequest.error,
      projectId: joinRequest.projectId,
    }
    this.emit('join-request-update', update)
  }

  /**
   * Get a join request by invite ID.
   *
   * @param {string} inviteId Hex invite ID
   * @returns {JoinRequest}
   * @throws {JoinRequestNotFoundError}
   */
  getJoinRequestById(inviteId) {
    const pending = this.#pending.get(inviteId)
    if (!pending) {
      throw new JoinRequestNotFoundError({ inviteId })
    }
    return pending.joinRequest
  }

  /**
   * Get all active (in-flight) join requests.
   *
   * @returns {JoinRequest[]}
   */
  getJoinRequests() {
    return [...this.#pending.values()].map((p) => p.joinRequest)
  }

  /**
   * Cancel an in-flight join request.
   *
   * @param {string} inviteId Hex invite ID
   * @param {Error} [reason] Reason for cancellation. Defaults to a generic cancellation error.
   * @returns {void}
   */
  cancelJoinRequest(inviteId, reason) {
    const pending = this.#pending.get(inviteId)
    if (!pending) {
      throw new JoinRequestNotFoundError({ inviteId })
    }
    pending.abortController.abort(reason ?? new JoinProjectCancelledError())
    this.#pending.delete(inviteId)
  }
}

/** @type {Set<string>} */
const RPC_DEVICE_TYPES = new Set(Object.values(DeviceInfo_DeviceType))

/**
 * The device type in our saved device info is the @comapeo/schema type, which
 * is a superset of the RPC enum; anything the RPC cannot express is sent as
 * unspecified.
 *
 * @param {string | undefined} deviceType
 * @returns {DeviceInfo_DeviceType}
 */
function toRpcDeviceType(deviceType) {
  if (
    deviceType &&
    deviceType !== DeviceInfo_DeviceType.UNRECOGNIZED &&
    RPC_DEVICE_TYPES.has(deviceType)
  ) {
    return /** @type {DeviceInfo_DeviceType} */ (deviceType)
  }
  return DeviceInfo_DeviceType.device_type_unspecified
}

/**
 * Wrap network/transport errors in an InviteConnectionError, pass others through.
 * @param {unknown} e
 * @returns {Error}
 */
function wrapNetworkError(e) {
  const err = ensureKnownError(e)
  if (NETWORK_ERROR_CODES.has(err.code)) {
    return new InviteConnectionError({ cause: err })
  }
  return err
}
