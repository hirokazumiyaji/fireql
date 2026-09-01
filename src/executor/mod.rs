mod batch;
mod doc_name;
mod insert_select;
mod select;

use crate::error::Result;
use crate::output::{FireqlOutput, FireqlStream};
use crate::sql::{SelectProjection, StatementAst};
use firestore::FirestoreDb;

use batch::{build_update_parts, execute_batch_write, BatchOp};
use insert_select::execute_insert_select;
use select::{execute_select, stream_select_rows};

pub async fn execute(
    db: &FirestoreDb,
    stmt: StatementAst,
    batch_parallelism: usize,
) -> Result<FireqlOutput> {
    match stmt {
        StatementAst::Select(select) => execute_select(db, select).await,
        StatementAst::Update(update) => {
            let op = BatchOp::Update(build_update_parts(
                &update.assignments,
                Some(db.get_documents_path().as_str()),
            )?);
            execute_batch_write(
                db,
                &update.collection,
                &update.filter,
                &update.order_by,
                update.limit,
                batch_parallelism,
                op,
            )
            .await
        }
        StatementAst::Delete(delete) => {
            execute_batch_write(
                db,
                &delete.collection,
                &delete.filter,
                &delete.order_by,
                delete.limit,
                batch_parallelism,
                BatchOp::Delete,
            )
            .await
        }
        StatementAst::InsertSelect(insert) => {
            execute_insert_select(db, insert, batch_parallelism).await
        }
    }
}

/// Streaming variant of [`execute`] (#55).
///
/// A plain SELECT (fields projection, no JOIN) is returned as
/// [`FireqlStream::Rows`], streaming documents as they arrive so callers can
/// keep their memory footprint constant. Aggregations, JOINed SELECTs, and
/// UPDATE/DELETE/INSERT SELECT statements run eagerly and are returned as
/// [`FireqlStream::Completed`].
pub async fn execute_stream<'a>(
    db: &'a FirestoreDb,
    stmt: StatementAst,
    batch_parallelism: usize,
) -> Result<FireqlStream<'a>> {
    match stmt {
        StatementAst::Select(select) if select.joins.is_none() => {
            if matches!(select.projection, SelectProjection::Fields(_)) {
                return Ok(FireqlStream::Rows(stream_select_rows(db, &select).await?));
            }
            Ok(FireqlStream::Completed(execute_select(db, select).await?))
        }
        other => Ok(FireqlStream::Completed(
            execute(db, other, batch_parallelism).await?,
        )),
    }
}
