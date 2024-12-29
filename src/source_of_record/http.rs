// use crate::source_of_record::SourceOfRecord;

// /// Implements an HTTP [`SourceOfRecord`] that respects cache control headers.
// pub struct HttpDataSourceOfRecord {}

// TODO: Implement the HTTP standard, decide how best to abstract this out independent of a particular client library
// impl HttpDataSourceOfRecord {}
//
// impl<Key, Value> SourceOfRecord<Key, Value> for HttpDataSourceOfRecord {
//     fn retrieve(&self, _key: &Key) -> Option<Value> {
//         todo!()
//     }
//
//     fn retrieve_with_hint(&self, _key: &Key, _current_value: &Value) -> Option<Value> {
//         todo!()
//     }
//
//     fn is_valid(&self, _key: &Key, _value: &Value) -> bool {
//         todo!()
//     }
// }
