use crate::datatype::memory_vecf32::Vecf32Output;
use crate::gucs::model::{
    openai_embedding_options,
    embedding_backend_options,
};
use base::vector::*;
use embedding::{embed, embed_batch, BackendOptions};
use pgrx::error;
use pgrx::iter::SetOfIterator;


#[pgrx::pg_extern(volatile, strict, parallel_safe)]
fn _vectors_text2vec(input: String) -> Vecf32Output {
    let backend = embedding_backend_options();

    let embedding_vec = embed(
        input,
        backend
    ).unwrap_or_else(|e| error!("{}", e.to_string()));

    Vecf32Output::new(VectBorrowed::new(&embedding_vec))
}

#[pgrx::pg_extern(volatile, strict, parallel_safe)]
fn _vectors_text2vec_array(
    inputs: Vec<String>
) -> SetOfIterator<'static, Vecf32Output> {
    let backend = embedding_backend_options();

    let embeddings = embed_batch(inputs, backend)
        .unwrap_or_else(|e| error!("{}", e.to_string()));

    SetOfIterator::new(
        embeddings
            .into_iter()
            .map(|vec| Vecf32Output::new(VectBorrowed::new(&vec)))
    )
}


#[pgrx::pg_extern(volatile, strict, parallel_safe)]
fn _vectors_text2vec_openai(input: String, model: String) -> Vecf32Output {
    let opts = openai_embedding_options();
    let embedding_vec = match embed(
        input,
        BackendOptions::OpenAI {
            base_url: opts.base_url,
            api_key: opts.api_key,
            model,
        },
    ) {
        Ok(v) => v,
        Err(e) => error!("{}", e.to_string()),
    };

    Vecf32Output::new(VectBorrowed::new(&embedding_vec))
}