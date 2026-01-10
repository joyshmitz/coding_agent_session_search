use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use hkdf::Hkdf;
use sha2::Sha256;

pub use argon2::Params as Argon2Params;

pub fn aes_gcm_encrypt(key: &[u8], nonce: &[u8], plaintext: &[u8], aad: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let key = Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(nonce);
    let payload = Payload {
        msg: plaintext,
        aad,
    };
    
    // aes-gcm returns ciphertext + tag appended.
    let ciphertext_with_tag = cipher.encrypt(nonce, payload).expect("encryption failure");
    
    // Tag is 16 bytes for AES-256-GCM
    let split_idx = ciphertext_with_tag.len() - 16;
    let (cipher, tag) = ciphertext_with_tag.split_at(split_idx);
    
    (cipher.to_vec(), tag.to_vec())
}

pub fn aes_gcm_decrypt(_key: &[u8], _nonce: &[u8], _ciphertext: &[u8], _aad: &[u8], _tag: &[u8]) -> Result<Vec<u8>, String> {
    // Placeholder
    Ok(vec![])
}

pub fn argon2id_hash(_password: &[u8], _salt: &[u8], _params: &Argon2Params) -> Vec<u8> {
    // Placeholder
    vec![]
}

pub fn hkdf_expand(ikm: &[u8], salt: &[u8], info: &[u8], len: usize) -> Vec<u8> {
    let hk = Hkdf::<Sha256>::new(Some(salt), ikm);
    let mut okm = vec![0u8; len];
    hk.expand(info, &mut okm).expect("hkdf expand failed");
    okm
}

pub fn hkdf_extract(_salt: &[u8], _ikm: &[u8]) -> Vec<u8> {
    // Placeholder
    vec![]
}
