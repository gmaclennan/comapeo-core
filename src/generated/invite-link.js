/* eslint-disable */
import _m0 from "protobufjs/minimal.js";
import { DeviceInfo_DeviceType, deviceInfo_DeviceTypeFromJSON, deviceInfo_DeviceTypeToNumber } from "./rpc.js";
export var Deny_DenyReason = {
    unspecified: "unspecified",
    unknown_invite_id: "unknown_invite_id",
    invitor_denied: "invitor_denied",
    /** invitor_error - The invitor failed to handle the request after acknowledging it */
    invitor_error: "invitor_error",
    UNRECOGNIZED: "UNRECOGNIZED",
};
export function deny_DenyReasonFromJSON(object) {
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
        case 3:
        case "invitor_error":
            return Deny_DenyReason.invitor_error;
        case -1:
        case "UNRECOGNIZED":
        default:
            return Deny_DenyReason.UNRECOGNIZED;
    }
}
export function deny_DenyReasonToNumber(object) {
    switch (object) {
        case Deny_DenyReason.unspecified:
            return 0;
        case Deny_DenyReason.unknown_invite_id:
            return 1;
        case Deny_DenyReason.invitor_denied:
            return 2;
        case Deny_DenyReason.invitor_error:
            return 3;
        case Deny_DenyReason.UNRECOGNIZED:
        default:
            return -1;
    }
}
function createBaseRedeem() {
    return { inviteId: Buffer.alloc(0), deviceName: "", deviceType: DeviceInfo_DeviceType.device_type_unspecified };
}
export var Redeem = {
    encode: function (message, writer) {
        if (writer === void 0) { writer = _m0.Writer.create(); }
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
    decode: function (input, length) {
        var reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
        var end = length === undefined ? reader.len : reader.pos + length;
        var message = createBaseRedeem();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
                case 1:
                    if (tag !== 10) {
                        break;
                    }
                    message.inviteId = reader.bytes();
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
    create: function (base) {
        return Redeem.fromPartial(base !== null && base !== void 0 ? base : {});
    },
    fromPartial: function (object) {
        var _a, _b, _c;
        var message = createBaseRedeem();
        message.inviteId = (_a = object.inviteId) !== null && _a !== void 0 ? _a : Buffer.alloc(0);
        message.deviceName = (_b = object.deviceName) !== null && _b !== void 0 ? _b : "";
        message.deviceType = (_c = object.deviceType) !== null && _c !== void 0 ? _c : DeviceInfo_DeviceType.device_type_unspecified;
        return message;
    },
};
function createBaseRedeemAck() {
    return { inviteId: Buffer.alloc(0) };
}
export var RedeemAck = {
    encode: function (message, writer) {
        if (writer === void 0) { writer = _m0.Writer.create(); }
        if (message.inviteId.length !== 0) {
            writer.uint32(10).bytes(message.inviteId);
        }
        return writer;
    },
    decode: function (input, length) {
        var reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
        var end = length === undefined ? reader.len : reader.pos + length;
        var message = createBaseRedeemAck();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
                case 1:
                    if (tag !== 10) {
                        break;
                    }
                    message.inviteId = reader.bytes();
                    continue;
            }
            if ((tag & 7) === 4 || tag === 0) {
                break;
            }
            reader.skipType(tag & 7);
        }
        return message;
    },
    create: function (base) {
        return RedeemAck.fromPartial(base !== null && base !== void 0 ? base : {});
    },
    fromPartial: function (object) {
        var _a;
        var message = createBaseRedeemAck();
        message.inviteId = (_a = object.inviteId) !== null && _a !== void 0 ? _a : Buffer.alloc(0);
        return message;
    },
};
function createBaseAdmit() {
    return { inviteId: Buffer.alloc(0) };
}
export var Admit = {
    encode: function (message, writer) {
        if (writer === void 0) { writer = _m0.Writer.create(); }
        if (message.inviteId.length !== 0) {
            writer.uint32(10).bytes(message.inviteId);
        }
        return writer;
    },
    decode: function (input, length) {
        var reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
        var end = length === undefined ? reader.len : reader.pos + length;
        var message = createBaseAdmit();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
                case 1:
                    if (tag !== 10) {
                        break;
                    }
                    message.inviteId = reader.bytes();
                    continue;
            }
            if ((tag & 7) === 4 || tag === 0) {
                break;
            }
            reader.skipType(tag & 7);
        }
        return message;
    },
    create: function (base) {
        return Admit.fromPartial(base !== null && base !== void 0 ? base : {});
    },
    fromPartial: function (object) {
        var _a;
        var message = createBaseAdmit();
        message.inviteId = (_a = object.inviteId) !== null && _a !== void 0 ? _a : Buffer.alloc(0);
        return message;
    },
};
function createBaseDeny() {
    return { inviteId: Buffer.alloc(0), reason: Deny_DenyReason.unspecified };
}
export var Deny = {
    encode: function (message, writer) {
        if (writer === void 0) { writer = _m0.Writer.create(); }
        if (message.inviteId.length !== 0) {
            writer.uint32(10).bytes(message.inviteId);
        }
        if (message.reason !== Deny_DenyReason.unspecified) {
            writer.uint32(16).int32(deny_DenyReasonToNumber(message.reason));
        }
        return writer;
    },
    decode: function (input, length) {
        var reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
        var end = length === undefined ? reader.len : reader.pos + length;
        var message = createBaseDeny();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
                case 1:
                    if (tag !== 10) {
                        break;
                    }
                    message.inviteId = reader.bytes();
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
    create: function (base) {
        return Deny.fromPartial(base !== null && base !== void 0 ? base : {});
    },
    fromPartial: function (object) {
        var _a, _b;
        var message = createBaseDeny();
        message.inviteId = (_a = object.inviteId) !== null && _a !== void 0 ? _a : Buffer.alloc(0);
        message.reason = (_b = object.reason) !== null && _b !== void 0 ? _b : Deny_DenyReason.unspecified;
        return message;
    },
};
function createBaseDenyAck() {
    return { inviteId: Buffer.alloc(0) };
}
export var DenyAck = {
    encode: function (message, writer) {
        if (writer === void 0) { writer = _m0.Writer.create(); }
        if (message.inviteId.length !== 0) {
            writer.uint32(10).bytes(message.inviteId);
        }
        return writer;
    },
    decode: function (input, length) {
        var reader = input instanceof _m0.Reader ? input : _m0.Reader.create(input);
        var end = length === undefined ? reader.len : reader.pos + length;
        var message = createBaseDenyAck();
        while (reader.pos < end) {
            var tag = reader.uint32();
            switch (tag >>> 3) {
                case 1:
                    if (tag !== 10) {
                        break;
                    }
                    message.inviteId = reader.bytes();
                    continue;
            }
            if ((tag & 7) === 4 || tag === 0) {
                break;
            }
            reader.skipType(tag & 7);
        }
        return message;
    },
    create: function (base) {
        return DenyAck.fromPartial(base !== null && base !== void 0 ? base : {});
    },
    fromPartial: function (object) {
        var _a;
        var message = createBaseDenyAck();
        message.inviteId = (_a = object.inviteId) !== null && _a !== void 0 ? _a : Buffer.alloc(0);
        return message;
    },
};
