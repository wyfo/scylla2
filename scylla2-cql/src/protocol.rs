use std::{collections::HashMap, net::SocketAddr};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::{
    error::{AuthenticationError, ConnectionError, FrameTooBig, InvalidRequest},
    frame::envelope::ENVELOPE_MAX_LENGTH,
    protocol::{auth::AuthenticationProtocol, read::read_envelope_v4},
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

async fn execute_v4(
    mut connection: impl AsyncWrite + AsyncRead + Unpin,
    version: ProtocolVersion,
    request: impl Request,
) -> Result<ResponseBody, ConnectionError> {
    let size = request
        .serialized_envelope_size(version, Default::default(), None)
        .map_err(InvalidRequest::from)?;
    if size > ENVELOPE_MAX_LENGTH {
        return Err(InvalidRequest::from(FrameTooBig(size)).into());
    }
    let mut buffer = vec![0; size];
    request.serialize_envelope(version, Default::default(), false, None, 0, &mut buffer);
    connection.write_all(&buffer).await?;
    connection.flush().await?;
    let envelope = read_envelope_v4(connection, None).await?;
    if envelope.stream != 0 {
        return Err(invalid_data("stream mismatch").into());
    }
    let response = Response::deserialize(version, Default::default(), envelope, None)?;
    Ok(response.ok()?.body)
}

pub async fn get_supported(
    mut connection: impl AsyncWrite + AsyncRead + Unpin,
    version: ProtocolVersion,
) -> Result<Supported, ConnectionError> {
    match execute_v4(&mut connection, version, Options).await {
        Ok(ResponseBody::Supported(supported)) => Ok(supported),
        Ok(other) => Err(invalid_response(other)),
        Err(err) => Err(err),
    }
}

pub async fn startup(
    mut connection: impl AsyncWrite + AsyncRead + Unpin,
    address: SocketAddr,
    version: ProtocolVersion,
    options: &HashMap<String, String>,
    authentication: Option<&dyn AuthenticationProtocol>,
) -> Result<(), ConnectionError> {
    let authenticator = match execute_v4(&mut connection, version, Startup { options }).await? {
        ResponseBody::Ready => return Ok(()),
        ResponseBody::Authenticate(auth) => auth.authenticator,
        other => return Err(invalid_response(other)),
    };
    let Some(auth) = authentication else {
        return Err(AuthenticationError::AuthenticationRequired(authenticator).into());
    };
    let (mut token, session) = auth.authenticate(&authenticator, address).await?;
    let mut session = session.unwrap_or_else(|| {
        Box::new(|_| async {
            Err(AuthenticationError::ChallengeRequested(
                authenticator.clone(),
            ))
        })
    });
    loop {
        match execute_v4(&mut connection, version, AuthResponse { token }).await? {
            ResponseBody::AuthSuccess(_) => return Ok(()),
            ResponseBody::AuthChallenge(AuthChallenge { token: tk, .. }) => {
                token = session.challenge(tk).await?
            }
            other => return Err(invalid_response(other)),
        }
    }
}
