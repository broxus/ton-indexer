// use ton_abi::{Function, Param, ParamType};
//
// use super::{FunctionArg, TokenValueExt};
//
// const ANSWER_ID: &str = "_answer_id";
//
// #[allow(dead_code)]
// #[derive(Default)]
// pub struct FunctionBuilder {
//     /// Contract function specification.
//     /// ABI version
//     abi_version: u8,
//     /// Function name.
//     name: String,
//     /// Function header parameters.
//     header: Vec<Param>,
//     /// Function input.
//     inputs: Vec<Param>,
//     /// Function output.
//     outputs: Vec<Param>,
//     /// Function ID for inbound messages
//     input_id: u32,
//     /// Function ID for outbound messages
//     output_id: u32,
//     /// Whether answer_id is set
//     responsible: bool,
// }
//
// impl FunctionBuilder {
//     pub fn new(function_name: &str) -> Self {
//         Self {
//             name: function_name.to_string(),
//             abi_version: 2,
//             ..Default::default()
//         }
//     }
//
//     pub fn new_responsible(function_name: &str) -> Self {
//         let mut function = Self::new(function_name);
//         function.make_responsible();
//         function
//     }
//
//     pub fn default_headers(self) -> Self {
//         self.header("pubkey", ParamType::PublicKey)
//             .header("time", ParamType::Time)
//             .header("expire", ParamType::Expire)
//     }
//
//     pub fn make_responsible(&mut self) {
//         if self.inputs.is_empty() {
//             self.inputs.push(Param::new(ANSWER_ID, ParamType::Uint(32)));
//         } else if !self.responsible {
//             self.inputs
//                 .insert(0, Param::new(ANSWER_ID, ParamType::Uint(32)));
//         }
//         self.responsible = true;
//     }
//
//     pub fn in_arg(mut self, name: &str, arg_type: ParamType) -> Self {
//         self.inputs.push(Param::new(name, arg_type));
//         self
//     }
//
//     pub fn inputs(mut self, inputs: Vec<Param>) -> Self {
//         self.inputs = inputs;
//         self
//     }
//
//     pub fn out_arg(mut self, name: &str, arg_type: ton_abi::ParamType) -> Self {
//         self.outputs.push(Param::new(name, arg_type));
//         self
//     }
//
//     pub fn outputs(mut self, outputs: Vec<Param>) -> Self {
//         self.outputs = outputs;
//         self
//     }
//
//     pub fn header(mut self, name: &str, arg_type: ton_abi::ParamType) -> Self {
//         self.header.push(Param::new(name, arg_type));
//         self
//     }
//
//     pub fn build(self) -> Function {
//         let mut fun = Function {
//             abi_version: self.abi_version,
//             name: self.name,
//             header: self.header,
//             inputs: self.inputs,
//             outputs: self.outputs,
//             input_id: 0,
//             output_id: 0,
//         };
//         let id = fun.get_function_id();
//         fun.input_id = id & 0x7FFFFFFF;
//         fun.output_id = id | 0x80000000;
//         fun
//     }
// }
//
// #[derive(Default)]
// pub struct TupleBuilder {
//     types: Vec<Param>,
// }
//
// impl TupleBuilder {
//     pub fn new() -> Self {
//         Self::default()
//     }
//
//     pub fn arg(mut self, name: &str, arg_type: ParamType) -> Self {
//         self.types.push(Param::new(name, arg_type));
//         self
//     }
//
//     pub fn build(self) -> ParamType {
//         ParamType::Tuple(self.types)
//     }
// }
//
// pub fn answer_id() -> ton_abi::Token {
//     0u32.token_value().named(ANSWER_ID)
// }
//
// #[cfg(test)]
// mod test {
//     use crate::helpers::abi::FunctionBuilder;
//     use ton_abi::ParamType;
//
//     // "name": "transfer",
//     // "inputs": [
//     // {"name":"to","type":"address"},
//     // {"name":"tokens","type":"uint128"},
//     // {"name":"grams","type":"uint128"},
//     // {"name":"send_gas_to","type":"address"},
//     // {"name":"notify_receiver","type":"bool"},
//     //     {"name":"payload","type":"cell"}
//     //     ],
//     //     "outputs": [
//     //     ]
//     #[test]
//     fn build() {
//         let original = crate::contracts::abi::ton_token_wallet_v3()
//             .function("transfer")
//             .unwrap();
//         let imposter = FunctionBuilder::new("transfer")
//             .default_headers()
//             .in_arg("to", ParamType::Address)
//             .in_arg("tokens", ParamType::Uint(128))
//             .in_arg("grams", ParamType::Uint(128))
//             .in_arg("send_gas_to", ParamType::Address)
//             .in_arg("notify_receiver", ParamType::Bool)
//             .in_arg("payload", ParamType::Cell)
//             .build();
//
//         assert_eq!(original, &imposter)
//     }
// }
