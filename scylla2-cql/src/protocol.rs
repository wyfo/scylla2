use std::{collections::HashMap, net::SocketAddr};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::{
    error::{AuthenticationError, ConnectionError, FrameTooBig, InvalidRequest},
    extensions::ProtocolExtensions,
    frame::envelope::ENVELOPE_MAX_LENGTH,
    protocol::{auth::AuthenticationProtocol, read::read_envelope, write::write_envelope},
    request::{auth::AuthResponse, options::Options, startup::Startup, Request, RequestExt},
    response::{auth::AuthChallenge, supported::Supported, Response, ResponseBody},
    utils::invalid_data,
    ProtocolVersion,
};

pub mod auth;
pub mod read;
pub mod write;

fn invalid_response(body: ResponseBody) -> ConnectionError {
    invalid_data(format!("invalid response: {body:?}")).into()
}

async fn execute_internal(
    mut connection: impl AsyncWrite + AsyncRead + Unpin,
    version: ProtocolVersion,
    extensions: Option<&ProtocolExtensions>,
    request: impl Request,
    before_startup: bool,
) -> Result<Response, ConnectionError> {
    let size = request
        .serialized_envelope_size(version, extensions, None)
        .map_err(InvalidRequest::from)?;
    if size > ENVELOPE_MAX_LENGTH {
        return Err(InvalidRequest::from(FrameTooBig(size)).into());
    }
    let mut envelope = vec![0; size];
    request.serialize_envelope(version, extensions, false, None, 0, &mut envelope);
    let execution_version = if before_startup {
        ProtocolVersion::V4
    } else {
        version
    };
    write_envelope(execution_version, false, &envelope, &mut connection).await?;
    connection.flush().await?;
    let envelope = read_envelope(execution_version, None, &mut connection).await?;
    if envelope.stream != 0 {
        return Err(invalid_data("stream mismatch").into());
    }
    let response = Response::deserialize(version, Default::default(), envelope, None)?;
    Ok(response.ok()?)
}

pub async fn execute_before_startup(
    connection: impl AsyncWrite + AsyncRead + Unpin,
    version: ProtocolVersion,
    extensions: Option<&ProtocolExtensions>,
    request: impl Request,
) -> Result<Response, ConnectionError> {
    execute_internal(connection, version, extensions, request, true).await
}

pub async fn execute(
    connection: impl AsyncWrite + AsyncRead + Unpin,
    version: ProtocolVersion,
    extensions: Option<&ProtocolExtensions>,
    request: impl Request,
) -> Result<Response, ConnectionError> {
    execute_internal(connection, version, extensions, request, false).await
}

pub async fn get_supported(
    mut connection: impl AsyncWrite + AsyncRead + Unpin,
    version: ProtocolVersion,
) -> Result<Supported, ConnectionError> {
    match execute_before_startup(&mut connection, version, Default::default(), Options)
        .await?
        .body
    {
        ResponseBody::Supported(supported) => Ok(supported),
        other => Err(invalid_response(other)),
    }
}

pub async fn startup(
    mut connection: impl AsyncWrite + AsyncRead + Unpin,
    address: SocketAddr,
    version: ProtocolVersion,
    options: &HashMap<String, String>,
    authentication: Option<&dyn AuthenticationProtocol>,
) -> Result<(), ConnectionError> {
    let authenticator = match execute_before_startup(
        &mut connection,
        version,
        Default::default(),
        Startup { options },
    )
    .await?
    .body
    {
        ResponseBody::Ready => return Ok(()),
        ResponseBody::Authenticate(auth) => auth.authenticator,
        other => return Err(invalid_response(other)),
    };
    let Some(auth) = authentication else {
        return Err(AuthenticationError::AuthenticationRequired(authenticator).into());
    };
    let (mut token, mut session) = auth.authenticate(&authenticator, address).await?;
    loop {
        let auth_res = execute_before_startup(
            &mut connection,
            version,
            Default::default(),
            AuthResponse { token },
        )
        .await?;
        match (auth_res.body, &mut session) {
            (ResponseBody::AuthSuccess(_), _) => return Ok(()),
            (ResponseBody::AuthChallenge(AuthChallenge { token: tk, .. }), Some(session)) => {
                token = session.challenge(tk).await?;
            }
            (ResponseBody::AuthChallenge(_), None) => {
                return Err(AuthenticationError::ChallengeRequested(authenticator.clone()).into())
            }

            (other, _) => return Err(invalid_response(other)),
        }
    }
}
