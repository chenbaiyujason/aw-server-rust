use rocket::form::Form;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::State;

use aw_models::BucketsExport;

use crate::config::AWConfig;
use crate::endpoints::forward_remote;
use crate::endpoints::{HttpErrorJson, ServerState};

fn import(
    state: &State<ServerState>,
    config: &State<AWConfig>,
    import: BucketsExport,
) -> Result<(), HttpErrorJson> {
    for (_bucketname, bucket) in import.buckets {
        let result = {
            let datastore = endpoints_get_lock!(state.datastore);
            datastore.create_bucket(&bucket)
        };
        match result {
            Ok(_) => (),
            Err(e) => {
                let err_msg = format!("Failed to import bucket: {e:?}");
                warn!("{}", err_msg);
                return Err(HttpErrorJson::new(Status::InternalServerError, err_msg));
            }
        }
        // import 会把 bucket 以及内嵌 events 一起落库，远端也需要收到同样的完整 bucket。
        forward_remote::maybe_forward_create_bucket(state, config, &bucket.id, &bucket);
    }
    Ok(())
}

#[post("/", data = "<json_data>", format = "application/json")]
pub fn bucket_import_json(
    state: &State<ServerState>,
    config: &State<AWConfig>,
    json_data: Json<BucketsExport>,
) -> Result<(), HttpErrorJson> {
    import(state, config, json_data.into_inner())
}

#[derive(FromForm)]
pub struct ImportForm {
    // FIXME: In the web-ui the name of this field is buckets.json, but "." is not allowed in field
    // names in Rocket and just simply "buckets" seems to work apparently but not sure why.
    // FIXME: In aw-server python it will import all fields rather just the one named
    // "buckets.json", that should probably be done here as well.
    #[field(name = "buckets")]
    import: Json<BucketsExport>,
}

#[post("/", data = "<form>", format = "multipart/form-data")]
pub fn bucket_import_form(
    state: &State<ServerState>,
    config: &State<AWConfig>,
    form: Form<ImportForm>,
) -> Result<(), HttpErrorJson> {
    import(state, config, form.into_inner().import.into_inner())
}
