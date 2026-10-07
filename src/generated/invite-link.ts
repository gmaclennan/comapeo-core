/* eslint-disable */
import _m0 from "protobufjs/minimal.js";
import { DeviceInfo_DeviceType, deviceInfo_DeviceTypeFromJSON, deviceInfo_DeviceTypeToNumber } from "./rpc.js";

/** Invitee -> invitor: request admission with an invite link ID */
export interface Redeem {
  inviteId: Buffer;
  /** Display info so the invitor can show who is asking before deciding */
  deviceName: string;
  /** 15: kept free for a request id, see the file comment */
  deviceType: DeviceInfo_DeviceType;
}

/** Invitor -> invitee: the redeem request was received and is awaiting a decision */
export interface RedeemAck {
  /** 15: kept free for a request id, see the file comment */
  inviteId: Buffer;
}

/** Invitor -> invitee: admission granted, both sides open the RPC channel */
export interface Admit {
  /** 15: kept free for a request id, see the file comment */
  inviteId: Buffer;
}

/** Invitor -> invitee: admission refused */
export interface Deny {
  inviteId: Buffer;
  /** 15: kept free for a request id, see the file comment */
  reason: Deny_DenyReason;
}

export const Deny_DenyReason = {
  unspecified: "unspecified",
  unknown_invite_id: "unknown_invite_id",
  invitor_denied: "invitor_denied",
  UNRECOGNIZED: "UNRECOGNIZED",
} as const;

export type Deny_DenyReason = typeof Deny_DenyReason[keyof typeof Deny_DenyReason];

export function deny_DenyReasonFromJSON(object: any): Deny_DenyReason {
  switch (object) {
    case 0:
    case "unspecified":
      return Deny_DenyReason.unspecified;
    case 1:
    case "unknown_invite_id":
      return Deny_DenyReason.unknown_invite_id;
    case 2:
    case "invitor_denied":
      return Deny_DenyReason.invitor_denied;
    case -1:
    case "UNRECOGNIZED":
    default:
      return Deny_DenyReason.UNRECOGNIZED;
  }
}

export function deny_DenyReasonToNumber(object: Deny_DenyReason): number {
  switch (object) {
    case Deny_DenyReason.unspecified:
      return 0;
    case Deny_DenyReason.unknown_invite_id:
      return 1;
    case Deny_DenyReason.invitor_denied:
      return 2;
    case Deny_DenyReason.UNRECOGNIZED:
    default:
      return -1;
  }
}

/** Invitee -> invitor: the deny was received, the invitor may now close */
export interface DenyAck {
  /** 15: kept free for a request id, see the file comment */
  inviteId: Buffer;
}

function createBaseRedeem(): Redeem {
  return { inviteId: Buffer.alloc(0), deviceName: "", deviceType: DeviceInfo_DeviceType.device_type_unspecified };
}

export const Redeem = {
  encode(message: Redeem, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.inviteId.length !== 0) {
      writer.uint32(10).bytes(message.inviteId);
    }
    if (message.deviceName !== "") {
      writer.uint32(18).string(message.deviceName);
    }
    if (message.deviceType !== DeviceInfo_DeviceType.device_type_unspecified) {
      writer.uint32(24).int32(deviceInfo_DeviceTypeToNumber(message.deviceType));
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Redeem {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRedeem();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.inviteId = reader.bytes() as Buffer;
          continue;
        case 2:
          if (tag !== 18) {
            break;
          }

          message.deviceName = reader.string();
          continue;
        case 3:
          if (tag !== 24) {
            break;
          }

          message.deviceType = deviceInfo_DeviceTypeFromJSON(reader.int32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  create<I extends Exact<DeepPartial<Redeem>, I>>(base?: I): Redeem {
    return Redeem.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<Redeem>, I>>(object: I): Redeem {
    const message = createBaseRedeem();
    message.inviteId = object.inviteId ?? Buffer.alloc(0);
    message.deviceName = object.deviceName ?? "";
    message.deviceType = object.deviceType ?? DeviceInfo_DeviceType.device_type_unspecified;
    return message;
  },
};

function createBaseRedeemAck(): RedeemAck {
  return { inviteId: Buffer.alloc(0) };
}

export const RedeemAck = {
  encode(message: RedeemAck, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.inviteId.length !== 0) {
      writer.uint32(10).bytes(message.inviteId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): RedeemAck {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseRedeemAck();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.inviteId = reader.bytes() as Buffer;
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  create<I extends Exact<DeepPartial<RedeemAck>, I>>(base?: I): RedeemAck {
    return RedeemAck.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<RedeemAck>, I>>(object: I): RedeemAck {
    const message = createBaseRedeemAck();
    message.inviteId = object.inviteId ?? Buffer.alloc(0);
    return message;
  },
};

function createBaseAdmit(): Admit {
  return { inviteId: Buffer.alloc(0) };
}

export const Admit = {
  encode(message: Admit, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.inviteId.length !== 0) {
      writer.uint32(10).bytes(message.inviteId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Admit {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseAdmit();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.inviteId = reader.bytes() as Buffer;
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  create<I extends Exact<DeepPartial<Admit>, I>>(base?: I): Admit {
    return Admit.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<Admit>, I>>(object: I): Admit {
    const message = createBaseAdmit();
    message.inviteId = object.inviteId ?? Buffer.alloc(0);
    return message;
  },
};

function createBaseDeny(): Deny {
  return { inviteId: Buffer.alloc(0), reason: Deny_DenyReason.unspecified };
}

export const Deny = {
  encode(message: Deny, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.inviteId.length !== 0) {
      writer.uint32(10).bytes(message.inviteId);
    }
    if (message.reason !== Deny_DenyReason.unspecified) {
      writer.uint32(16).int32(deny_DenyReasonToNumber(message.reason));
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): Deny {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDeny();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.inviteId = reader.bytes() as Buffer;
          continue;
        case 2:
          if (tag !== 16) {
            break;
          }

          message.reason = deny_DenyReasonFromJSON(reader.int32());
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  create<I extends Exact<DeepPartial<Deny>, I>>(base?: I): Deny {
    return Deny.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<Deny>, I>>(object: I): Deny {
    const message = createBaseDeny();
    message.inviteId = object.inviteId ?? Buffer.alloc(0);
    message.reason = object.reason ?? Deny_DenyReason.unspecified;
    return message;
  },
};

function createBaseDenyAck(): DenyAck {
  return { inviteId: Buffer.alloc(0) };
}

export const DenyAck = {
  encode(message: DenyAck, writer: _m0.Writer = _m0.Writer.create()): _m0.Writer {
    if (message.inviteId.length !== 0) {
      writer.uint32(10).bytes(message.inviteId);
    }
    return writer;
  },

  decode(input: _m0.Reader | Uint8Array, length?: number): DenyAck {
    const reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
    let end = length === undefined ? reader.len : reader.pos + length;
    const message = createBaseDenyAck();
    while (reader.pos < end) {
      const tag = reader.uint32();
      switch (tag >>> 3) {
        case 1:
          if (tag !== 10) {
            break;
          }

          message.inviteId = reader.bytes() as Buffer;
          continue;
      }
      if ((tag & 7) === 4 || tag === 0) {
        break;
      }
      reader.skipType(tag & 7);
    }
    return message;
  },

  create<I extends Exact<DeepPartial<DenyAck>, I>>(base?: I): DenyAck {
    return DenyAck.fromPartial(base ?? ({} as any));
  },
  fromPartial<I extends Exact<DeepPartial<DenyAck>, I>>(object: I): DenyAck {
    const message = createBaseDenyAck();
    message.inviteId = object.inviteId ?? Buffer.alloc(0);
    return message;
  },
};

type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;

type DeepPartial<T> = T extends Builtin ? T
  : T extends Array<infer U> ? Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>>
  : T extends {} ? { [K in keyof T]?: DeepPartial<T[K]> }
  : Partial<T>;

type KeysOfUnion<T> = T extends T ? keyof T : never;
type Exact<P, I extends P> = P extends Builtin ? P
  : P & { [K in keyof P]: Exact<P[K], I[K]> } & { [K in Exclude<keyof I, KeysOfUnion<P>>]: never };
