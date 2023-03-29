use actix_web::{web, HttpResponse};
use actix_web_macros::{delete, get, post, put};
use chrono::{DateTime, Utc};
use log::error;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use crate::error::{Error, ResponseError};
use crate::helpers::Authentication;
use crate::routes::IndexParam;
use crate::Data;

pub fn services(cfg: &mut web::ServiceConfig) {
    cfg.service(list_indexes)
        .service(get_index)
        .service(create_index)
        .service(update_index)
        .service(delete_index)
        .service(get_update_status)
        .service(get_all_updates_status);
}

fn generate_uid() -> String {
    let mut rng = rand::thread_rng();
    let sample = b"abcdefghijklmnopqrstuvwxyz0123456789";
    sample
        .choose_multiple(&mut rng, 8)
        .map(|c| *c as char)
        .collect()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct IndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

#[get("/indexes", wrap = "Authentication::Private")]
async fn list_indexes(data: web::Data<Data>) -> Result<HttpResponse, ResponseError> {
    let reader = data.db.main_read_txn()?;
    let mut indexes = Vec::new();

    for index_uid in data.db.indexes_uids() {
        let index = data.db.open_index(&index_uid);

        match index {
            Some(index) => {
                let name = index.main.name(&reader)?.ok_or(Error::internal(
                        "Impossible to get the name of an index",
                ))?;
                let created_at = index
                    .main
                    .created_at(&reader)?
                    .ok_or(Error::internal(
                            "Impossible to get the create date of an index",
                    ))?;
                let updated_at = index
                    .main
                    .updated_at(&reader)?
                    .ok_or(Error::internal(
                            "Impossible to get the last update date of an index",
                    ))?;

                let primary_key = match index.main.schema(&reader) {
                    Ok(Some(schema)) => match schema.primary_key() {
                        Some(primary_key) => Some(primary_key.to_owned()),
                        None => None,
                    },
                    _ => None,
                };

                let index_response = IndexResponse {
                    name,
                    uid: index_uid,
                    created_at,
                    updated_at,
                    primary_key,
                };
                indexes.push(index_response);
            }
            None => error!(
                "Index {} is referenced in the indexes list but cannot be found",
                index_uid
            ),
        }
    }

    Ok(HttpResponse::Ok().json(indexes))
}

#[get("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn get_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let reader = data.db.main_read_txn()?;
    let name = index.main.name(&reader)?.ok_or(Error::internal(
            "Impossible to get the name of an index",
    ))?;
    let created_at = index
        .main
        .created_at(&reader)?
        .ok_or(Error::internal(
                "Impossible to get the create date of an index",
        ))?;
    let updated_at = index
        .main
        .updated_at(&reader)?
        .ok_or(Error::internal(
                "Impossible to get the last update date of an index",
        ))?;

    let primary_key = match index.main.schema(&reader) {
        Ok(Some(schema)) => match schema.primary_key() {
            Some(primary_key) => Some(primary_key.to_owned()),
            None => None,
        },
        _ => None,
    };
    let index_response = IndexResponse {
        name,
        uid: path.index_uid.clone(),
        created_at,
        updated_at,
        primary_key,
    };

    Ok(HttpResponse::Ok().json(index_response))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IndexCreateRequest {
    name: Option<String>,
    uid: Option<String>,
    primary_key: Option<String>,
}

#[post("/indexes", wrap = "Authentication::Private")]
async fn create_index(
    data: web::Data<Data>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    if let (None, None) = (body.name.clone(), body.uid.clone()) {
        return Err(Error::bad_request(
            "Index creation must have an uid",
        ).into());
    }

    let uid = match &body.uid {
        Some(uid) => {
            if uid
                .chars()
                .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
            {
                uid.to_owned()
            } else {
                return Err(Error::InvalidIndexUid.into());
            }
        }
        None => loop {
            let uid = generate_uid();
            if data.db.open_index(&uid).is_none() {
                break uid;
            }
        },
    };

    let created_index = data
        .db
        .create_index(&uid)
        .map_err(|e| match e {
            meilisearch_core::Error::IndexAlreadyExists => e.into(),
            _ => ResponseError::from(Error::create_index(e))
        })?;

    let index_response = data.db.main_write::<_, _, ResponseError>(|mut writer| {
        let name = body.name.as_ref().unwrap_or(&uid);
        created_index.main.put_name(&mut writer, name)?;

        let created_at = created_index
            .main
            .created_at(&writer)?
            .ok_or(Error::internal("Impossible to read created at"))?;

        let updated_at = created_index
            .main
            .updated_at(&writer)?
            .ok_or(Error::internal("Impossible to read updated at"))?;

        if let Some(id) = body.primary_key.clone() {
            if let Some(mut schema) = created_index.main.schema(&writer)? {
                schema
                    .set_primary_key(&id)
                    .map_err(Error::bad_request)?;
                created_index.main.put_schema(&mut writer, &schema)?;
            }
        }
        let index_response = IndexResponse {
            name: name.to_string(),
            uid,
            created_at,
            updated_at,
            primary_key: body.primary_key.clone(),
        };
        Ok(index_response)
    })?;

    Ok(HttpResponse::Created().json(index_response))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UpdateIndexRequest {
    name: Option<String>,
    primary_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateIndexResponse {
    name: String,
    uid: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

#[put("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn update_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
    body: web::Json<IndexCreateRequest>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    data.db.main_write::<_, _, ResponseError>(|writer| {
        if let Some(name) = &body.name {
            index.main.put_name(writer, name)?;
        }

        if let Some(id) = body.primary_key.clone() {
            if let Some(mut schema) = index.main.schema(writer)? {
                schema.set_primary_key(&id)?;
                index.main.put_schema(writer, &schema)?;
            }
        }
        index.main.put_updated_at(writer)?;
        Ok(())
    })?;

    let reader = data.db.main_read_txn()?;
    let name = index.main.name(&reader)?.ok_or(Error::internal(
            "Impossible to get the name of an index",
    ))?;
    let created_at = index
        .main
        .created_at(&reader)?
        .ok_or(Error::internal(
                "Impossible to get the create date of an index",
        ))?;
    let updated_at = index
        .main
        .updated_at(&reader)?
        .ok_or(Error::internal(
                "Impossible to get the last update date of an index",
        ))?;

    let primary_key = match index.main.schema(&reader) {
        Ok(Some(schema)) => match schema.primary_key() {
            Some(primary_key) => Some(primary_key.to_owned()),
            None => None,
        },
        _ => None,
    };

    let index_response = IndexResponse {
        name,
        uid: path.index_uid.clone(),
        created_at,
        updated_at,
        primary_key,
    };

    Ok(HttpResponse::Ok().json(index_response))
}

#[delete("/indexes/{index_uid}", wrap = "Authentication::Private")]
async fn delete_index(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    if data.db.delete_index(&path.index_uid)? {
        Ok(HttpResponse::NoContent().finish())
    } else {
        Err(Error::index_not_found(&path.index_uid).into())
    }
}

#[derive(Deserialize)]
struct UpdateParam {
    index_uid: String,
    update_id: u64,
}

#[get(
    "/indexes/{index_uid}/updates/{update_id}",
    wrap = "Authentication::Private"
)]
async fn get_update_status(
    data: web::Data<Data>,
    path: web::Path<UpdateParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let reader = data.db.update_read_txn()?;

    let status = index.update_status(&reader, path.update_id)?;

    match status {
        Some(status) => Ok(HttpResponse::Ok().json(status)),
        None => Err(Error::NotFound(format!(
            "Update {}",
            path.update_id
        )).into()),
    }
}

#[get("/indexes/{index_uid}/updates", wrap = "Authentication::Private")]
async fn get_all_updates_status(
    data: web::Data<Data>,
    path: web::Path<IndexParam>,
) -> Result<HttpResponse, ResponseError> {
    let index = data
        .db
        .open_index(&path.index_uid)
        .ok_or(Error::index_not_found(&path.index_uid))?;

    let reader = data.db.update_read_txn()?;

    let response = index.all_updates_status(&reader)?;

    Ok(HttpResponse::Ok().json(response))
}
