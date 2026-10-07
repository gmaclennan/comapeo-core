import _m0 from "protobufjs/minimal.js";
import { DeviceInfo_DeviceType } from "./rpc.js";
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
export declare const Deny_DenyReason: {
    readonly unspecified: "unspecified";
    readonly unknown_invite_id: "unknown_invite_id";
    readonly invitor_denied: "invitor_denied";
    /** invitor_error - The invitor failed to handle the request after acknowledging it */
    readonly invitor_error: "invitor_error";
    readonly UNRECOGNIZED: "UNRECOGNIZED";
};
export type Deny_DenyReason = typeof Deny_DenyReason[keyof typeof Deny_DenyReason];
export declare function deny_DenyReasonFromJSON(object: any): Deny_DenyReason;
export declare function deny_DenyReasonToNumber(object: Deny_DenyReason): number;
/** Invitee -> invitor: the deny was received, the invitor may now close */
export interface DenyAck {
    /** 15: kept free for a request id, see the file comment */
    inviteId: Buffer;
}
export declare const Redeem: {
    encode(message: Redeem, writer?: _m0.Writer): _m0.Writer;
    decode(input: _m0.Reader | Uint8Array, length?: number): Redeem;
    create<I extends Exact<DeepPartial<Redeem>, I>>(base?: I): Redeem;
    fromPartial<I extends Exact<DeepPartial<Redeem>, I>>(object: I): Redeem;
};
export declare const RedeemAck: {
    encode(message: RedeemAck, writer?: _m0.Writer): _m0.Writer;
    decode(input: _m0.Reader | Uint8Array, length?: number): RedeemAck;
    create<I extends Exact<DeepPartial<RedeemAck>, I>>(base?: I): RedeemAck;
    fromPartial<I extends Exact<DeepPartial<RedeemAck>, I>>(object: I): RedeemAck;
};
export declare const Admit: {
    encode(message: Admit, writer?: _m0.Writer): _m0.Writer;
    decode(input: _m0.Reader | Uint8Array, length?: number): Admit;
    create<I extends Exact<DeepPartial<Admit>, I>>(base?: I): Admit;
    fromPartial<I extends Exact<DeepPartial<Admit>, I>>(object: I): Admit;
};
export declare const Deny: {
    encode(message: Deny, writer?: _m0.Writer): _m0.Writer;
    decode(input: _m0.Reader | Uint8Array, length?: number): Deny;
    create<I extends Exact<DeepPartial<Deny>, I>>(base?: I): Deny;
    fromPartial<I extends Exact<DeepPartial<Deny>, I>>(object: I): Deny;
};
export declare const DenyAck: {
    encode(message: DenyAck, writer?: _m0.Writer): _m0.Writer;
    decode(input: _m0.Reader | Uint8Array, length?: number): DenyAck;
    create<I extends Exact<DeepPartial<DenyAck>, I>>(base?: I): DenyAck;
    fromPartial<I extends Exact<DeepPartial<DenyAck>, I>>(object: I): DenyAck;
};
type Builtin = Date | Function | Uint8Array | string | number | boolean | undefined;
type DeepPartial<T> = T extends Builtin ? T : T extends Array<infer U> ? Array<DeepPartial<U>> : T extends ReadonlyArray<infer U> ? ReadonlyArray<DeepPartial<U>> : T extends {} ? {
    [K in keyof T]?: DeepPartial<T[K]>;
} : Partial<T>;
type KeysOfUnion<T> = T extends T ? keyof T : never;
type Exact<P, I extends P> = P extends Builtin ? P : P & {
    [K in keyof P]: Exact<P[K], I[K]>;
} & {
    [K in Exclude<keyof I, KeysOfUnion<P>>]: never;
};
export {};
