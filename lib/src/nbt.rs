/// Little-endian NBT codec for Bedrock level.dat.
///
/// The file format is:
///   [u32 LE] storage_version
///   [u32 LE] payload_length
///   [NBT]    root TAG_Compound (name is always empty string)
///
/// JSON representation uses a tagged-value scheme so types round-trip exactly:
///   {"byte": N}, {"short": N}, {"int": N}, {"long": N},
///   {"float": N}, {"double": N},
///   {"string": "..."}, {"byte_array": "base64..."}, {"int_array": [...]},
///   {"long_array": [...]}, {"compound": {name: value, ...}},
///   {"list": {"of": "type_name", "v": [...]}}
use std::io::{Cursor, Read};

use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde_json::{Map, Value};

const HEADER_LEN: usize = 8;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub fn level_dat_to_json(data: &[u8]) -> Result<Value> {
    if data.len() < HEADER_LEN {
        bail!("level.dat too short ({} bytes)", data.len());
    }
    let storage_version = u32::from_le_bytes(data[0..4].try_into()?);

    let mut cur = Cursor::new(&data[HEADER_LEN..]);
    let tag_type = read_u8(&mut cur)?;
    if tag_type != 10 {
        bail!("Expected TAG_Compound (10) at root, got {tag_type}");
    }
    let _root_name = read_string(&mut cur)?;
    let fields = read_compound_payload(&mut cur)?;

    let mut root = Map::new();
    root.insert("_storage_version".into(), Value::Number(storage_version.into()));
    root.insert("_root".into(), Value::Object(fields));
    Ok(Value::Object(root))
}

pub fn json_to_level_dat(json: &Value) -> Result<Vec<u8>> {
    let obj = json.as_object().ok_or_else(|| anyhow!("Expected JSON object"))?;

    let storage_version = obj
        .get("_storage_version")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("Missing _storage_version"))? as u32;

    let root_fields = obj
        .get("_root")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("Missing _root"))?;

    let mut payload = Vec::new();
    write_u8(&mut payload, 10); // TAG_Compound
    write_string(&mut payload, ""); // empty root name
    write_compound_payload(&mut payload, root_fields)?;

    let mut out = Vec::new();
    out.extend_from_slice(&storage_version.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

fn read_u8(c: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut buf = [0u8; 1];
    c.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_i8(c: &mut Cursor<&[u8]>) -> Result<i8> {
    Ok(read_u8(c)? as i8)
}

fn read_le_i16(c: &mut Cursor<&[u8]>) -> Result<i16> {
    let mut buf = [0u8; 2];
    c.read_exact(&mut buf)?;
    Ok(i16::from_le_bytes(buf))
}

fn read_le_u16(c: &mut Cursor<&[u8]>) -> Result<u16> {
    let mut buf = [0u8; 2];
    c.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_le_i32(c: &mut Cursor<&[u8]>) -> Result<i32> {
    let mut buf = [0u8; 4];
    c.read_exact(&mut buf)?;
    Ok(i32::from_le_bytes(buf))
}

fn read_le_i64(c: &mut Cursor<&[u8]>) -> Result<i64> {
    let mut buf = [0u8; 8];
    c.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}

fn read_le_f32(c: &mut Cursor<&[u8]>) -> Result<f32> {
    let bits = read_le_i32(c)? as u32;
    Ok(f32::from_bits(bits))
}

fn read_le_f64(c: &mut Cursor<&[u8]>) -> Result<f64> {
    let bits = read_le_i64(c)? as u64;
    Ok(f64::from_bits(bits))
}

fn read_string(c: &mut Cursor<&[u8]>) -> Result<String> {
    let len = read_le_u16(c)? as usize;
    let mut buf = vec![0u8; len];
    c.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

fn tagged(key: &str, val: Value) -> Value {
    let mut m = Map::new();
    m.insert(key.into(), val);
    Value::Object(m)
}

/// Read a tag payload (no name, no type prefix).
fn read_payload(c: &mut Cursor<&[u8]>, tag_type: u8) -> Result<Value> {
    let v = match tag_type {
        1 => tagged("byte", (read_i8(c)? as i64).into()),
        2 => tagged("short", (read_le_i16(c)? as i64).into()),
        3 => tagged("int", (read_le_i32(c)? as i64).into()),
        4 => tagged("long", read_le_i64(c)?.into()),
        5 => {
            let hex = format!("{:08x}", read_le_f32(c)?.to_bits());
            tagged("float", Value::String(hex))
        }
        6 => {
            let hex = format!("{:016x}", read_le_f64(c)?.to_bits());
            tagged("double", Value::String(hex))
        }
        7 => {
            let len = read_le_i32(c)?;
            if len < 0 { bail!("Negative byte array length"); }
            let mut buf = vec![0u8; len as usize];
            c.read_exact(&mut buf)?;
            tagged("byte_array", Value::String(B64.encode(&buf)))
        }
        8 => tagged("string", Value::String(read_string(c)?)),
        9 => {
            let elem_type = read_u8(c)?;
            let count = read_le_i32(c)?;
            if count < 0 { bail!("Negative list length"); }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push(read_payload(c, elem_type)?);
            }
            let mut list_obj = Map::new();
            list_obj.insert("of".into(), Value::String(tag_type_name(elem_type).into()));
            list_obj.insert("v".into(), Value::Array(items));
            tagged("list", Value::Object(list_obj))
        }
        10 => tagged("compound", Value::Object(read_compound_payload(c)?)),
        11 => {
            let len = read_le_i32(c)?;
            if len < 0 { bail!("Negative int_array length"); }
            let items: Result<Vec<Value>> = (0..len).map(|_| Ok(read_le_i32(c)?.into())).collect();
            tagged("int_array", Value::Array(items?))
        }
        12 => {
            let len = read_le_i32(c)?;
            if len < 0 { bail!("Negative long_array length"); }
            let items: Result<Vec<Value>> = (0..len).map(|_| Ok(read_le_i64(c)?.into())).collect();
            tagged("long_array", Value::Array(items?))
        }
        t => bail!("Unknown NBT tag type: {t}"),
    };
    Ok(v)
}

fn read_compound_payload(c: &mut Cursor<&[u8]>) -> Result<Map<String, Value>> {
    let mut map = Map::new();
    loop {
        let tag_type = read_u8(c)?;
        if tag_type == 0 {
            // TAG_End
            break;
        }
        let name = read_string(c)?;
        let value = read_payload(c, tag_type)?;
        map.insert(name, value);
    }
    Ok(map)
}

fn tag_type_name(tag: u8) -> &'static str {
    match tag {
        0 => "end",   // TAG_End as list element type = empty list
        1 => "byte",
        2 => "short",
        3 => "int",
        4 => "long",
        5 => "float",
        6 => "double",
        7 => "byte_array",
        8 => "string",
        9 => "list",
        10 => "compound",
        11 => "int_array",
        12 => "long_array",
        _ => "unknown",
    }
}

fn tag_type_id(name: &str) -> Result<u8> {
    Ok(match name {
        "end" => 0,
        "byte" => 1,
        "short" => 2,
        "int" => 3,
        "long" => 4,
        "float" => 5,
        "double" => 6,
        "byte_array" => 7,
        "string" => 8,
        "list" => 9,
        "compound" => 10,
        "int_array" => 11,
        "long_array" => 12,
        other => bail!("Unknown NBT type name: {other}"),
    })
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

fn write_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

fn write_le_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_le_i16(out: &mut Vec<u8>, v: i16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_le_i32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_le_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    write_le_u16(out, bytes.len() as u16);
    out.extend_from_slice(bytes);
}

/// Write a tagged payload given its type-tagged JSON value.
fn write_tagged_value(out: &mut Vec<u8>, val: &Value) -> Result<()> {
    let obj = val.as_object().ok_or_else(|| anyhow!("Expected tagged NBT object, got {val}"))?;
    let (type_name, inner) = obj
        .iter()
        .next()
        .ok_or_else(|| anyhow!("Empty tagged NBT object"))?;

    match type_name.as_str() {
        "byte" => {
            let n = inner.as_i64().ok_or_else(|| anyhow!("byte must be integer"))? as i8;
            write_u8(out, n as u8);
        }
        "short" => {
            let n = inner.as_i64().ok_or_else(|| anyhow!("short must be integer"))? as i16;
            write_le_i16(out, n);
        }
        "int" => {
            let n = inner.as_i64().ok_or_else(|| anyhow!("int must be integer"))? as i32;
            write_le_i32(out, n);
        }
        "long" => {
            let n = inner.as_i64().ok_or_else(|| anyhow!("long must be integer"))?;
            write_le_i64(out, n);
        }
        "float" => {
            let hex = inner.as_str().ok_or_else(|| anyhow!("float must be hex string"))?;
            let bits = u32::from_str_radix(hex, 16)?;
            write_le_i32(out, bits as i32);
        }
        "double" => {
            let hex = inner.as_str().ok_or_else(|| anyhow!("double must be hex string"))?;
            let bits = u64::from_str_radix(hex, 16)?;
            write_le_i64(out, bits as i64);
        }
        "byte_array" => {
            let b64 = inner.as_str().ok_or_else(|| anyhow!("byte_array must be base64 string"))?;
            let bytes = B64.decode(b64)?;
            write_le_i32(out, bytes.len() as i32);
            out.extend_from_slice(&bytes);
        }
        "string" => {
            let s = inner.as_str().ok_or_else(|| anyhow!("string must be a string"))?;
            write_string(out, s);
        }
        "list" => {
            let list_obj = inner.as_object().ok_or_else(|| anyhow!("list value must be object"))?;
            let elem_type_name = list_obj
                .get("of")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("list missing 'of'"))?;
            let elem_type_id = tag_type_id(elem_type_name)?;
            let items = list_obj
                .get("v")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow!("list missing 'v'"))?;
            write_u8(out, elem_type_id);
            write_le_i32(out, items.len() as i32);
            for item in items {
                write_payload_for_type(out, item, elem_type_name)?;
            }
        }
        "compound" => {
            let fields = inner.as_object().ok_or_else(|| anyhow!("compound must be object"))?;
            write_compound_payload(out, fields)?;
        }
        "int_array" => {
            let items = inner.as_array().ok_or_else(|| anyhow!("int_array must be array"))?;
            write_le_i32(out, items.len() as i32);
            for item in items {
                let n = item.as_i64().ok_or_else(|| anyhow!("int_array element must be integer"))? as i32;
                write_le_i32(out, n);
            }
        }
        "long_array" => {
            let items = inner.as_array().ok_or_else(|| anyhow!("long_array must be array"))?;
            write_le_i32(out, items.len() as i32);
            for item in items {
                let n = item.as_i64().ok_or_else(|| anyhow!("long_array element must be integer"))?;
                write_le_i64(out, n);
            }
        }
        other => bail!("Unknown NBT type key in JSON: {other}"),
    }
    Ok(())
}

/// Write a payload inline (no name/type prefix), used for list elements.
/// The element type name is given separately since list items have no type prefix.
fn write_payload_for_type(out: &mut Vec<u8>, val: &Value, type_name: &str) -> Result<()> {
    // For compound and list elements, the values inside lists ARE tagged objects
    // (because we represent them as {"compound":{...}} or {"list":{...}} etc.)
    // For primitive types in lists (byte, short, int, long, float, double, string)
    // the list value is also a tagged object: {"int": 42}.
    // We just delegate to write_tagged_value since we always use tagged objects.
    let _ = type_name; // the tag is embedded in val itself
    write_tagged_value(out, val)
}

fn write_compound_payload(out: &mut Vec<u8>, fields: &Map<String, Value>) -> Result<()> {
    for (name, val) in fields {
        // Determine tag type from the value
        let tag_type_id = get_tag_type_id(val)?;
        write_u8(out, tag_type_id);
        write_string(out, name);
        write_tagged_value(out, val)?;
    }
    write_u8(out, 0); // TAG_End
    Ok(())
}

fn get_tag_type_id(val: &Value) -> Result<u8> {
    let obj = val.as_object().ok_or_else(|| anyhow!("Expected tagged NBT object"))?;
    let type_name = obj.keys().next().ok_or_else(|| anyhow!("Empty tagged NBT object"))?;
    tag_type_id(type_name)
}

// ---------------------------------------------------------------------------
// Raw NBT (no level.dat header) - used for misc values and chunk NBT tags
// ---------------------------------------------------------------------------

/// Read a single NBT compound from raw bytes (no level.dat 8-byte header).
/// The bytes begin directly with the tag type byte (0x0A), name, and payload.
pub fn read_raw_nbt(data: &[u8]) -> Result<Value> {
    let mut cur = Cursor::new(data);
    let tag_type = read_u8(&mut cur)?;
    if tag_type != 10 {
        bail!("Expected TAG_Compound (10) at start of raw NBT, got {tag_type}");
    }
    let _name = read_string(&mut cur)?;
    let fields = read_compound_payload(&mut cur)?;
    Ok(Value::Object(fields))
}

/// Write a single NBT compound to raw bytes (no level.dat header).
pub fn write_raw_nbt(value: &Value) -> Result<Vec<u8>> {
    let fields = value.as_object().ok_or_else(|| anyhow!("Expected JSON object for raw NBT"))?;
    let mut out = Vec::new();
    write_u8(&mut out, 10);
    write_string(&mut out, "");
    write_compound_payload(&mut out, fields)?;
    Ok(out)
}

/// Read a back-to-back sequence of NBT compounds from raw bytes.
/// Used for chunk block_entities (tag 49), entities (tag 50), pending_ticks (tag 51).
/// Returns a JSON array of decoded compound objects.
pub fn read_nbt_sequence(data: &[u8]) -> Result<Vec<Value>> {
    let mut cur = Cursor::new(data);
    let mut items = Vec::new();
    while (cur.position() as usize) < data.len() {
        let tag_type = read_u8(&mut cur)?;
        if tag_type != 10 {
            bail!("Expected TAG_Compound (10) in sequence, got {tag_type}");
        }
        let _name = read_string(&mut cur)?;
        let fields = read_compound_payload(&mut cur)?;
        items.push(Value::Object(fields));
    }
    Ok(items)
}

/// Write a sequence of NBT compounds back to raw bytes.
pub fn write_nbt_sequence(values: &[Value]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for val in values {
        let fields = val.as_object().ok_or_else(|| anyhow!("Expected JSON object in NBT sequence"))?;
        write_u8(&mut out, 10);
        write_string(&mut out, "");
        write_compound_payload(&mut out, fields)?;
    }
    Ok(out)
}
