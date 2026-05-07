pub(crate) const AUTH_TYPE_NTLM: u8 = 10;
pub(crate) const AUTH_LEVEL_CONNECT: u8 = 2;

#[derive(Debug, Clone)]
pub(crate) struct NtlmVerifier {
    pub(crate) auth_type: u8,
    pub(crate) auth_level: u8,
    pub(crate) context_id: u32,
    pub(crate) value: Vec<u8>,
}

pub(crate) fn connect_level_challenge_verifier() -> NtlmVerifier {
    NtlmVerifier {
        auth_type: AUTH_TYPE_NTLM,
        auth_level: AUTH_LEVEL_CONNECT,
        context_id: 0,
        value: challenge_token(),
    }
}

fn challenge_token() -> Vec<u8> {
    const NTLMSSP_NEGOTIATE_UNICODE: u32 = 0x0000_0001;
    const NTLMSSP_REQUEST_TARGET: u32 = 0x0000_0004;
    const NTLMSSP_NEGOTIATE_NTLM: u32 = 0x0000_0200;
    const NTLMSSP_NEGOTIATE_ALWAYS_SIGN: u32 = 0x0000_8000;
    const NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY: u32 = 0x0008_0000;
    const NTLM_CHALLENGE: [u8; 8] = [0x4c, 0x50, 0x45, 0x52, 0x43, 0x41, 0x30, 0x31];
    const SECURITY_BUFFER_OFFSET: u32 = 48;

    let flags = NTLMSSP_NEGOTIATE_UNICODE
        | NTLMSSP_REQUEST_TARGET
        | NTLMSSP_NEGOTIATE_NTLM
        | NTLMSSP_NEGOTIATE_ALWAYS_SIGN
        | NTLMSSP_NEGOTIATE_EXTENDED_SESSIONSECURITY;

    let mut token = Vec::with_capacity(SECURITY_BUFFER_OFFSET as usize);
    token.extend_from_slice(b"NTLMSSP\0");
    token.extend_from_slice(&2u32.to_le_bytes());
    token.extend_from_slice(&0u16.to_le_bytes());
    token.extend_from_slice(&0u16.to_le_bytes());
    token.extend_from_slice(&SECURITY_BUFFER_OFFSET.to_le_bytes());
    token.extend_from_slice(&flags.to_le_bytes());
    token.extend_from_slice(&NTLM_CHALLENGE);
    token.extend_from_slice(&[0u8; 8]);
    token.extend_from_slice(&0u16.to_le_bytes());
    token.extend_from_slice(&0u16.to_le_bytes());
    token.extend_from_slice(&SECURITY_BUFFER_OFFSET.to_le_bytes());
    token
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_level_challenge_verifier_is_ntlm_type2() {
        let verifier = connect_level_challenge_verifier();

        assert_eq!(verifier.auth_type, AUTH_TYPE_NTLM);
        assert_eq!(verifier.auth_level, AUTH_LEVEL_CONNECT);
        assert_eq!(verifier.context_id, 0);
        assert_eq!(verifier.value.len(), 48);
        assert_eq!(&verifier.value[0..8], b"NTLMSSP\0");
        assert_eq!(
            u32::from_le_bytes([
                verifier.value[8],
                verifier.value[9],
                verifier.value[10],
                verifier.value[11]
            ]),
            2
        );
    }
}
