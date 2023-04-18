// const { json } = require('express');
const bs58 = require('bs58');
const BN = require('bn.js');
const Buffer = require('buffer').Buffer;
const { Connection, LAMPORTS_PER_SOL} = require('@solana/web3.js');
const { publicKey, u64 } = require('@solana/buffer-layout-utils');
const { blob,  u8, u32, nu64, ns64, struct, seq } = require('@solana/buffer-layout'); // Layout
// import BN from 'bn.js';
// import {Buffer} from 'buffer';
require('dotenv').config();
const Pool = require('pg').Pool
const pool = new Pool({
    user: process.env.PGUSERNAME,
    host: process.env.PGHOSTNAME,
    database: process.env.PGDATABASE,
    password: process.env.PGPASSWORD,
    port: process.env.PGPORT,
  });

// system program interfaces
const TransferLayout = struct([
  u32 ('discriminator'),
  u64('lamports'),
]);

const CreateAccountLayout = struct([
  u32 ('discriminator'),
  u64('lamports'),
  u64('space'),
  u32('owner'),
]);

const CreateAccountWithSeedLayout = struct([
  u32('discriminator'),
  publicKey('base'),
  u8('seedLength'),
  blob(24, 'seed'), 
  // seq(u8(), 24, 'seed'), 
  u64('lamports'), 
  u64('space'), 
  publicKey('owner'), 
]);

// stake program interfaces
const AuthorizedLayout = struct([
  publicKey('staker'),
  publicKey('withdrawer')
])
const LockupLayout = struct([
  ns64('unix_timestamp'),
  nu64('epoch'),
  publicKey('custodian')
]);
const StakeInitializeLayout = struct([
  u32('discriminator'), 
  AuthorizedLayout.replicate('authorized'),
  LockupLayout.replicate('lockup'),
]);

const WithdrawLayout = struct([
  u32 ('discriminator'),
  u64('lamports'),
]);

// spl-token interfaces
const TokenTransferLayout = struct([
  u8('discriminator'),
  u64('amount'),
]);

const TokenTransferCheckedLayout = struct([
  u8('discriminator'),
  u64('amount'),
  u8('decimals'),
]);

let programMap = new Map([
  ["11111111111111111111111111111111", "system"],
  ["Stake11111111111111111111111111111111111111", "stake"],
  ["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "spl-token"]
]);

// lookup by position in the enum here:
// - system = https://docs.rs/solana-program/latest/src/solana_program/system_instruction.rs.html#201
// - stake =
let MethodMap = new Map([
  ["system_0", "createAccount"],
  ["system_1", "assign"],
  ["system_2", "transfer"],
  ["system_3", "createAccountWithSeed"],
  ["system_4", "advanceNonceAccount"],
  ["stake_4", "withdraw"], // confirmed
  ["stake_0", "initialize"], // confirmed
  ["stake_5", "deactivate"], // confirmed
  ["stake_2", "delegate"], // confirmed
  ["spl-token_3", "transfer"],
  ["spl-token_12", "transferChecked"],
]);

const insertData = async (fields, values) => {
  return new Promise(function(resolve, reject) {
      const QUERY_TEXT = `INSERT INTO stake_program_event_log(${fields}) VALUES(${values});`
      console.log(QUERY_TEXT)
      pool.query(QUERY_TEXT, (error, results) => {
        if (error) {
          reject(error)
          console.log("insert FAILED!");
        }
        resolve(results.rows);
        console.log(`inserted sig OK: ${signature}`);
      })
  });
}

const insertParsedTransaction = (req) => {
  
  try {
      const data = req.body[0];
      // console.log(JSON.parse(JSON.stringify(data)));
      const slot = data?.slot;
      const blocktime = data?.blockTime;
      const err = data?.meta.err;
      const fee = data?.meta.fee / LAMPORTS_PER_SOL;
      const signature = data?.transaction.signatures[0];
      data?.transaction.message.instructions.map(async (instruction, index) => {
          const ix = bs58.decode(instruction.data)
          const prefix = ix.slice(0,4);
          const disc = (Buffer.from(prefix)).readUInt32LE()
          const programAddress = data?.transaction.message.accountKeys[instruction.programIdIndex].toString()
          const program = programMap.get(programAddress);
          const instructionType = MethodMap.get(`${program}_${disc}`)

          // console.log(`accountKeys: ${
          //     data?.transaction.message.accountKeys.map(key => {
          //         return key
          //     })
          // }`)
          if (program == 'stake') {
              if (instructionType == 'initialize') {
                  const decodedData = StakeInitializeLayout.decode(ix);
                  const staker = decodedData.authorized.staker
                  const withdrawer = decodedData.authorized.withdrawer
                  const custodian = decodedData.lockup.custodian
                  const stakeAccount = data?.transaction.message.accountKeys[instruction.accounts[0]];
                  const epoch = decodedData.lockup.epoch
                  const unixTimestamp = decodedData.lockup.unix_timestamp
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','authority2','authority3','destination','misc1','misc2'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${staker.toBase58()}'`,`'${withdrawer.toBase58()}'`,`'${custodian.toBase58()}'`,`'${stakeAccount.toBase58()}'`,epoch,unixTimestamp];
                  insertData(fields, values);
                  console.log(`${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${staker},${withdrawer},${custodian},,${stakeAccount},,${epoch},${unixTimestamp},`);
              } 
              else if (instructionType == 'delegate') {
                  const stakeAccount = data?.transaction.message.accountKeys[instruction.accounts[0]];
                  const stakeAuthority = data?.transaction.message.accountKeys[instruction.accounts[5]];
                  const voteAccount = data?.transaction.message.accountKeys[instruction.accounts[1]];
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','destination','destination2'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${stakeAuthority.toBase58()}'`,`'${stakeAccount.toBase58()}'`,`'${voteAccount.toBase58()}'`];
                  insertData(fields, values);
                  console.log(`${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${stakeAuthority},,,,${stakeAccount},${voteAccount},,,`);
              } 
              else if (instructionType == 'deactivate') {
                  const stakeAuthority = data?.transaction.message.accountKeys[instruction.accounts[2]]
                  const stakeAccount = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','source']
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${stakeAuthority.toBase58()}'`,`'${stakeAccount.toBase58()}'`];
                  insertData(fields, values);
                  console.log(`${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${stakeAuthority},,,${stakeAccount},,,,,`);
              } 
              else if (instructionType == 'withdraw') {
                  const deserialized = WithdrawLayout.decode(ix);
                  const lamports = Number(deserialized.lamports);
                  const uiAmount = lamports / LAMPORTS_PER_SOL
                  const from = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const to = data?.transaction.message.accountKeys[instruction.accounts[1]]
                  const withdrawAuthority = data?.transaction.message.accountKeys[instruction.accounts[4]]
                  const fields = ['program', 'type', 'signature', 'err', 'slot', 'blocktime', 'fee', 'authority2', 'source', 'destination', 'uiAmount']
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${withdrawAuthority.toBase58()}'`,`'${from.toBase58()}'`,`'${to.toBase58()}'`,uiAmount];
                  insertData(fields, values);
                  console.log(`${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},,${withdrawAuthority},,${from.toString()},${to.toString()},,,,${uiAmount}`);
              }
          } 
          else if (program == 'system'){
              if (instructionType == 'createAccount') {
                  const deserialized = CreateAccountLayout.decode(ix);
                  const lamports = Number(deserialized.lamports);
                  const uiAmount = lamports / LAMPORTS_PER_SOL
                  const from = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const to = data?.transaction.message.accountKeys[instruction.accounts[1]]
                  const fields = ['program','type','signature','err','slot','blocktime','fee','source','destination','uiAmount'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${from.toBase58()}'`,`'${to.toBase58()}'`,uiAmount];
                  insertData(fields, values);
                  console.log(`${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},,,,${from},${to},,,,${uiAmount}`);
              }
              else if (instructionType == 'createAccountWithSeed') {
                  const deserialized = CreateAccountWithSeedLayout.decode(ix);
                  lamports = data?.meta.postBalances[instruction.accounts[1]] - data?.meta.preBalances[instruction.accounts[1]];
                  const uiAmount = lamports / LAMPORTS_PER_SOL
                  const space = Number(deserialized.space);
                  const seed = 'seed unavailable';
                  const from = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const to = data?.transaction.message.accountKeys[instruction.accounts[1]]
                  const fields = ['program','type','signature','err','slot','blocktime','fee','source','destination','misc1','uiAmount']
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${from}'`,`'${to}'`,`'${seed}'`,`'${uiAmount}'`];
                  insertData(fields, values);
                  console.log(`${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},,,,${from},${to},,${seed},,${uiAmount}`);

              }
              else if (instructionType == 'transfer') {
                  const deserialized = TransferLayout.decode(ix);
                  const lamports = Number(deserialized.lamports);
                  const uiAmount = lamports / LAMPORTS_PER_SOL
                  const from = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const to = data?.transaction.message.accountKeys[instruction.accounts[1]]
                  const fields = ['program','type','signature','err','slot','blocktime','fee','source','destination','uiAmount'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${from.toBase58()}'`,`'${to.toBase58()}'`,uiAmount];
                  insertData(fields, values);
                  console.log(`${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},,,,${from},${to},,,,${uiAmount}`);
              }
          } 
          else if (program == 'spl-token') {
              const prefix2 = ix.slice(0,1);
              const disc = (Buffer.from(prefix2)).readUInt8()
              const instructionType = MethodMap.get(`${program}_${disc}`)
              const source = data?.transaction.message.accountKeys[instruction.accounts[0]]
              const mint = data?.transaction.message.accountKeys[instruction.accounts[1]]
              const destination = data?.transaction.message.accountKeys[instruction.accounts[2]]
              const authority = data?.transaction.message.accountKeys[instruction.accounts[3]]
              if (instructionType == 'transfer') {
                  const deserialized = TokenTransferLayout.decode(ix);
                  const amount = Number(deserialized.amount); // wrong
                  const decimals = 0 // Number(deserialized.decimals); // wrong
                  const uiAmount = amount // / 10 ** decimals;
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','source','destination','destination2','misc1','misc2','uiAmount']
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${authority.toBase58()}'`,`'${source.toBase58()}'`,`'${destination.toBase58()}'`,`'${mint.toBase58()}'`,decimals,uiAmount];
                  insertData(fields, values);
                  console.log(`${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${authority},,,${source},,${destination},${mint},${decimals},${uiAmount}`);
              } 
              else if (instructionType == 'transferChecked') {
                  const deserialized = TokenTransferCheckedLayout.decode(ix);
                  const amount = Number(deserialized.amount);
                  const decimals = Number(deserialized.decimals);
                  const uiAmount = amount / 10 ** decimals;
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','source','destination2','misc1','misc2','uiAmount']
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${authority.toBase58()}'`,`'${source.toBase58()}'`,`'${destination.toBase58()}'`,`'${mint.toBase58()}'`,decimals,uiAmount];
                  insertData(fields, values);
                  console.log(`${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${authority},,,${source},,${destination},${mint},${decimals},${uiAmount}`);
              }
          }
          else {
              // console.log(`No result: ${signature} not a stake-related tx`);
          }
      });
  } catch (err) {
      console.log(err);
  }
    

}

const test = () => {

  return new Promise(function(resolve, reject) {
    pool.query(`select * from hello_world;`, (error, results) => {
      if (error) {
        reject(error)
      }
      resolve(results.rows);
      console.log("test OK");
    })
  }) 
}

module.exports = {
    insertParsedTransaction,
    test
  }