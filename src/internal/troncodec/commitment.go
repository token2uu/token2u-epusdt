// Package troncodec verifies the data used by the HTTP payment scanner against
// TRON's protobuf transaction and block commitments. Schema field numbers follow
// https://github.com/tronprotocol/protocol/blob/master/core/Tron.proto.
package troncodec

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var schemas = sync.OnceValues(buildSchemas)

func field(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type, typeName string, repeated bool) *descriptorpb.FieldDescriptorProto {
	label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	if repeated {
		label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	}
	f := &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Type: &kind, Label: &label}
	if typeName != "" {
		f.TypeName = proto.String(typeName)
	}
	return f
}
func enum(name string, values ...string) *descriptorpb.EnumDescriptorProto {
	e := &descriptorpb.EnumDescriptorProto{Name: proto.String(name)}
	for i, v := range values {
		e.Value = append(e.Value, &descriptorpb.EnumValueDescriptorProto{Name: proto.String(v), Number: proto.Int32(int32(i))})
	}
	return e
}
func buildSchemas() (protoreflect.FileDescriptor, error) {
	i64, b, s, m, e := descriptorpb.FieldDescriptorProto_TYPE_INT64, descriptorpb.FieldDescriptorProto_TYPE_BYTES, descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, descriptorpb.FieldDescriptorProto_TYPE_ENUM
	header := &descriptorpb.DescriptorProto{Name: proto.String("Header"), Field: []*descriptorpb.FieldDescriptorProto{
		field("timestamp", 1, i64, "", false), field("txTrieRoot", 2, b, "", false), field("parentHash", 3, b, "", false), field("number", 7, i64, "", false), field("witness_id", 8, i64, "", false), field("witness_address", 9, b, "", false), field("version", 10, descriptorpb.FieldDescriptorProto_TYPE_INT32, "", false), field("accountStateRoot", 11, b, "", false),
	}}
	detail := &descriptorpb.DescriptorProto{Name: proto.String("OrderDetail"), Field: []*descriptorpb.FieldDescriptorProto{field("makerOrderId", 1, b, "", false), field("takerOrderId", 2, b, "", false), field("fillSellQuantity", 3, i64, "", false), field("fillBuyQuantity", 4, i64, "", false)}}
	result := &descriptorpb.DescriptorProto{Name: proto.String("Result"), EnumType: []*descriptorpb.EnumDescriptorProto{
		enum("Code", "SUCESS", "FAILED"), enum("ContractResult", "DEFAULT", "SUCCESS", "REVERT", "BAD_JUMP_DESTINATION", "OUT_OF_MEMORY", "PRECOMPILED_CONTRACT", "STACK_TOO_SMALL", "STACK_TOO_LARGE", "ILLEGAL_OPERATION", "STACK_OVERFLOW", "OUT_OF_ENERGY", "OUT_OF_TIME", "JVM_STACK_OVER_FLOW", "UNKNOWN", "TRANSFER_FAILED", "INVALID_CODE"),
	}, Field: []*descriptorpb.FieldDescriptorProto{
		field("fee", 1, i64, "", false), field("ret", 2, e, ".troncommit.Result.Code", false), field("contractRet", 3, e, ".troncommit.Result.ContractResult", false), field("assetIssueID", 14, s, "", false), field("withdraw_amount", 15, i64, "", false), field("unfreeze_amount", 16, i64, "", false), field("exchange_received_amount", 18, i64, "", false), field("exchange_inject_another_amount", 19, i64, "", false), field("exchange_withdraw_another_amount", 20, i64, "", false), field("exchange_id", 21, i64, "", false), field("shielded_transaction_fee", 22, i64, "", false), field("orderId", 25, b, "", false), field("orderDetails", 26, m, ".troncommit.OrderDetail", true), field("withdraw_expire_amount", 27, i64, "", false),
	}}
	return protodesc.NewFile(&descriptorpb.FileDescriptorProto{Name: proto.String("tron_commitment.proto"), Package: proto.String("troncommit"), Syntax: proto.String("proto3"), MessageType: []*descriptorpb.DescriptorProto{header, detail, result}}, nil)
}

func hexBytes(v gjson.Result) ([]byte, error) {
	if v.Type != gjson.String {
		return nil, fmt.Errorf("expected hexadecimal string")
	}
	return hex.DecodeString(strings.TrimPrefix(v.String(), "0x"))
}
func encodeMessage(name string, obj map[string]json.RawMessage) ([]byte, error) {
	schema, err := schemas()
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	msg := dynamicpb.NewMessage(schema.Messages().ByName(protoreflect.Name(name)))
	if err := protojson.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("invalid TRON %s protobuf JSON: %w", name, err)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(msg)
}
func jsonObject(v gjson.Result) (map[string]json.RawMessage, error) {
	if !v.IsObject() {
		return nil, fmt.Errorf("expected TRON object")
	}
	var obj map[string]json.RawMessage
	err := json.Unmarshal([]byte(v.Raw), &obj)
	return obj, err
}
func convertHex(obj map[string]json.RawMessage, keys ...string) error {
	for _, key := range keys {
		raw, ok := obj[key]
		if !ok {
			continue
		}
		b, err := hexBytes(gjson.ParseBytes(raw))
		if err != nil {
			return err
		}
		obj[key], _ = json.Marshal(base64.StdEncoding.EncodeToString(b))
	}
	return nil
}
func appendBytes(dst []byte, tag protowire.Number, value []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(dst, tag, protowire.BytesType), value)
}
func encodeResult(v gjson.Result) ([]byte, error) {
	obj, err := jsonObject(v)
	if err != nil {
		return nil, err
	}
	if err := convertHex(obj, "orderId"); err != nil {
		return nil, err
	}
	if details, ok := obj["orderDetails"]; ok {
		var entries []json.RawMessage
		if err := json.Unmarshal(details, &entries); err != nil {
			return nil, err
		}
		for i, entry := range entries {
			d, err := jsonObject(gjson.ParseBytes(entry))
			if err != nil {
				return nil, err
			}
			if err := convertHex(d, "makerOrderId", "takerOrderId"); err != nil {
				return nil, err
			}
			entries[i], _ = json.Marshal(d)
		}
		obj["orderDetails"], _ = json.Marshal(entries)
	}
	// java-tron's JsonFormat prints maps as ordered key/value arrays. Also
	// support protobuf-JSON objects, preserving the provider's entry order.
	mapValue := v.Get("cancel_unfreezeV2_amount")
	delete(obj, "cancel_unfreezeV2_amount")
	encoded, err := encodeMessage("Result", obj)
	if err != nil {
		return nil, err
	}
	if mapValue.Exists() {
		appendEntry := func(key string, val gjson.Result) bool {
			var amount int64
			if val.Exists() {
				var e error
				amount, e = strconv.ParseInt(val.String(), 10, 64)
				if e != nil || (val.Type != gjson.Number && val.Type != gjson.String) {
					err = fmt.Errorf("invalid TRON unfreeze amount")
					return false
				}
			}
			entry := appendBytes(nil, 1, []byte(key))
			entry = protowire.AppendVarint(protowire.AppendTag(entry, 2, protowire.VarintType), uint64(amount))
			encoded = appendBytes(encoded, 28, entry)
			return true
		}
		switch {
		case mapValue.IsObject():
			mapValue.ForEach(func(k, val gjson.Result) bool { return appendEntry(k.String(), val) })
		case mapValue.IsArray():
			for _, row := range mapValue.Array() {
				key := row.Get("key")
				if !row.IsObject() || (key.Exists() && key.Type != gjson.String) {
					return nil, fmt.Errorf("invalid TRON unfreeze map entry")
				}
				for name := range row.Map() {
					if name != "key" && name != "value" {
						return nil, fmt.Errorf("unknown TRON unfreeze map field")
					}
				}
				if !appendEntry(key.String(), row.Get("value")) {
					break
				}
			}
		default:
			return nil, fmt.Errorf("invalid TRON unfreeze map")
		}
	}
	return encoded, err
}

// Verify checks both the header/block ID binding and the complete transaction
// Merkle root (including signatures and results, not just transaction IDs).
func Verify(block gjson.Result) error {
	if err := validateJSONKeys([]byte(block.Raw)); err != nil {
		return err
	}
	header := block.Get("block_header.raw_data")
	obj, err := jsonObject(header)
	if err != nil {
		return err
	}
	if err := convertHex(obj, "txTrieRoot", "parentHash", "witness_address", "accountStateRoot"); err != nil {
		return err
	}
	headerBytes, err := encodeMessage("Header", obj)
	if err != nil {
		return err
	}
	id := sha256.Sum256(headerBytes)
	height, err := strconv.ParseUint(header.Get("number").Raw, 10, 64)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(id[:8], height)
	givenID, err := hexBytes(block.Get("blockID"))
	if err != nil || !bytes.Equal(givenID, id[:]) {
		return fmt.Errorf("TRON block ID does not match header")
	}
	root, err := hexBytes(header.Get("txTrieRoot"))
	if err != nil || len(root) != 32 {
		return fmt.Errorf("invalid TRON Merkle root")
	}
	var leaves [][32]byte
	for _, tx := range block.Get("transactions").Array() {
		raw, err := hexBytes(tx.Get("raw_data_hex"))
		if err != nil || len(raw) == 0 {
			return fmt.Errorf("missing TRON raw transaction")
		}
		hash := sha256.Sum256(raw)
		givenHash, err := hexBytes(tx.Get("txID"))
		if err != nil || !bytes.Equal(givenHash, hash[:]) {
			return fmt.Errorf("TRON transaction ID does not match raw data")
		}
		if err := verifyPaymentFields(tx.Get("raw_data"), raw); err != nil {
			return err
		}
		encoded := appendBytes(nil, 1, raw)
		sigs := tx.Get("signature")
		if sigs.Exists() && !sigs.IsArray() {
			return fmt.Errorf("invalid TRON signatures")
		}
		for _, sig := range sigs.Array() {
			b, err := hexBytes(sig)
			if err != nil {
				return err
			}
			encoded = appendBytes(encoded, 2, b)
		}
		for _, result := range tx.Get("ret").Array() {
			b, err := encodeResult(result)
			if err != nil {
				return err
			}
			encoded = appendBytes(encoded, 5, b)
		}
		leaves = append(leaves, sha256.Sum256(encoded))
	}
	var actual [32]byte
	if len(leaves) > 0 {
		for len(leaves) > 1 {
			next := make([][32]byte, 0, (len(leaves)+1)/2)
			for i := 0; i < len(leaves); i += 2 {
				if i+1 == len(leaves) {
					next = append(next, leaves[i])
					continue
				}
				var pair [64]byte
				copy(pair[:32], leaves[i][:])
				copy(pair[32:], leaves[i+1][:])
				next = append(next, sha256.Sum256(pair[:]))
			}
			leaves = next
		}
		actual = leaves[0]
	}
	if !bytes.Equal(root, actual[:]) {
		return fmt.Errorf("TRON transaction Merkle root mismatch")
	}
	return nil
}

type wireValue struct {
	kind   protowire.Type
	data   []byte
	number uint64
}

func decodeWire(raw []byte) (map[protowire.Number][]wireValue, error) {
	out := make(map[protowire.Number][]wireValue)
	for len(raw) > 0 {
		tag, kind, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		raw = raw[n:]
		v := wireValue{kind: kind}
		size := 0
		switch kind {
		case protowire.BytesType:
			v.data, size = protowire.ConsumeBytes(raw)
		case protowire.VarintType:
			v.number, size = protowire.ConsumeVarint(raw)
		default:
			size = protowire.ConsumeFieldValue(tag, kind, raw)
		}
		if size < 0 {
			return nil, protowire.ParseError(size)
		}
		raw = raw[size:]
		out[tag] = append(out[tag], v)
	}
	return out, nil
}
func single(fields map[protowire.Number][]wireValue, tag protowire.Number, kind protowire.Type) (wireValue, error) {
	entries := fields[tag]
	if len(entries) == 0 {
		return wireValue{kind: kind}, nil
	} // protobuf default value
	if len(entries) != 1 || entries[0].kind != kind {
		return wireValue{}, fmt.Errorf("invalid TRON protobuf field %d", tag)
	}
	return entries[0], nil
}
func verifyPaymentFields(jsonRaw gjson.Result, raw []byte) error {
	fields, err := decodeWire(raw)
	if err != nil {
		return err
	}
	contracts := jsonRaw.Get("contract").Array()
	if len(fields[11]) != len(contracts) {
		return fmt.Errorf("TRON contract list differs from raw data")
	}
	for i, contract := range fields[11] {
		if contract.kind != protowire.BytesType {
			return fmt.Errorf("invalid TRON contract encoding")
		}
		c, err := decodeWire(contract.data)
		if err != nil {
			return err
		}
		typ, err := single(c, 1, protowire.VarintType)
		if err != nil {
			return err
		}
		jsonKind := contracts[i].Get("type").String()
		if typ.number != 1 && typ.number != 31 {
			if jsonKind == "TransferContract" || jsonKind == "TriggerSmartContract" {
				return fmt.Errorf("TRON contract type mismatch")
			}
			continue
		}
		expected := "TransferContract"
		if typ.number == 31 {
			expected = "TriggerSmartContract"
		}
		if jsonKind != expected {
			return fmt.Errorf("TRON payment contract type mismatch")
		}
		parameter, err := single(c, 2, protowire.BytesType)
		if err != nil {
			return err
		}
		anyFields, err := decodeWire(parameter.data)
		if err != nil {
			return err
		}
		value, err := single(anyFields, 2, protowire.BytesType)
		if err != nil {
			return err
		}
		values, err := decodeWire(value.data)
		if err != nil {
			return err
		}
		names := []string{"owner_address", "to_address"}
		if typ.number == 31 {
			names[1] = "contract_address"
		}
		jsonValue := contracts[i].Get("parameter.value")
		for j, name := range names {
			wire, err := single(values, protowire.Number(j+1), protowire.BytesType)
			if err != nil {
				return err
			}
			supplied, err := hexBytes(jsonValue.Get(name))
			if err != nil || !bytes.Equal(supplied, wire.data) {
				return fmt.Errorf("TRON payment address mismatch")
			}
		}
		if typ.number == 1 {
			amount, err := single(values, 3, protowire.VarintType)
			if err != nil {
				return err
			}
			supplied, err := strconv.ParseInt(jsonValue.Get("amount").Raw, 10, 64)
			if err != nil || supplied != int64(amount.number) {
				return fmt.Errorf("TRON payment amount mismatch")
			}
		} else {
			input, err := single(values, 4, protowire.BytesType)
			if err != nil {
				return err
			}
			var supplied []byte
			if value := jsonValue.Get("data"); value.Exists() {
				supplied, err = hexBytes(value)
			}
			if err != nil || !bytes.Equal(supplied, input.data) {
				return fmt.Errorf("TRON contract input mismatch")
			}
		}
	}
	return nil
}
