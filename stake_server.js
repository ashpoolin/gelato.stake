// const { json } = require('express');
const bs58 = require('bs58');
const sha256 = require('crypto-js/sha256');
const BN = require('bn.js');
const Buffer = require('buffer').Buffer;
const { Connection, LAMPORTS_PER_SOL, PublicKey} = require('@solana/web3.js');

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
const SplitLayout = struct([
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

// const AuthorizeLayout = struct([
  // u8('discriminator'),
  // publicKey('custodian'),
  // u8('authorityType'),
// ]);
// 
// const AuthorizeCheckedLayout = struct([
  // u8('discriminator'),
  // u8('authorizeWithCustodian'),
// ]);

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
  ["stake_0", "initialize"], // confirmed
  ["stake_1", "authorize"], // confirmed
  ["stake_2", "delegate"], // confirmed
  ["stake_3", "split"], // confirmed
  ["stake_4", "withdraw"], // confirmed
  ["stake_5", "deactivate"], // confirmed
  ["stake_7", "merge"], // confirmed
  ["stake_10", "authorizeChecked"], // confirmed
  ["spl-token_3", "transfer"],
  ["spl-token_12", "transferChecked"],
]);

const insertData = (signature, fields, values) => {
  return new Promise(function(resolve, reject) {
      const QUERY_TEXT = `INSERT INTO stake_program_event_log(${fields}) VALUES(${values}) ON CONFLICT DO NOTHING;`
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

const hashMessage = (message) => {
  const hash = sha256(message).toString();
  const encodedHash = bs58.encode(Buffer.from(hash, 'hex'));
  return encodedHash;
}

const insertParsedTransaction = (req) => {
  
  let promises = [];
  try {
      const data = req.body[0];
      // console.log(JSON.parse(JSON.stringify(data)));
      const slot = data?.slot;
      const blocktime = data?.blockTime;
      const err = data?.meta.err;
      const fee = data?.meta.fee / LAMPORTS_PER_SOL;
      const signature = data?.transaction.signatures[0];
      promises = data?.transaction.message.instructions.map(async (instruction, index) => {
          const programAddress = data?.transaction.message.accountKeys[instruction.programIdIndex].toString()
          const program = programMap.get(programAddress);
          
          const ix = bs58.decode(instruction.data);
          let disc;
          try {
            if (program === 'spl-token') {
              disc = ix.slice(0,1);
            } else {
              disc = (Buffer.from(ix.slice(0,4))).readUInt32LE()
            }
          } catch (err) {
            disc = 999;
          }
          
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
                  const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${staker},${withdrawer},${custodian},,${stakeAccount},,${epoch},${unixTimestamp},`;
                  console.log(message);
                  const encodedHash = hashMessage(message);
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','authority2','authority3','destination','misc1','misc2','serial'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(staker)).toBase58()}'`,`'${(new PublicKey(withdrawer)).toBase58()}'`,`'${(new PublicKey(custodian)).toBase58()}'`,`'${(new PublicKey(stakeAccount)).toBase58()}'`,epoch,unixTimestamp,`'${encodedHash}'`];
                  return insertData(signature, fields, values);
              } 
              else if (instructionType == 'delegate') {
                  const stakeAccount = data?.transaction.message.accountKeys[instruction.accounts[0]];
                  const stakeAuthority = data?.transaction.message.accountKeys[instruction.accounts[5]];
                  const voteAccount = data?.transaction.message.accountKeys[instruction.accounts[1]];
                  const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${stakeAuthority},,,,${stakeAccount},${voteAccount},,,`;
                  console.log(message);
                  const encodedHash = hashMessage(message);
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','destination','destination2','serial'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(stakeAuthority)).toBase58()}'`,`'${(new PublicKey(stakeAccount)).toBase58()}'`,`'${(new PublicKey(voteAccount)).toBase58()}'`,`'${encodedHash}'`];
                  return insertData(signature, fields, values);
              } 
              else if (instructionType == 'deactivate') {
                  const stakeAuthority = data?.transaction.message.accountKeys[instruction.accounts[2]]
                  const stakeAccount = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${stakeAuthority},,,${stakeAccount},,,,,`;
                  console.log(message);
                  const encodedHash = hashMessage(message);
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','source','serial'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(stakeAuthority)).toBase58()}'`,`'${(new PublicKey(stakeAccount)).toBase58()}'`,`'${encodedHash}'`];
                  return insertData(signature, fields, values);
              } 
              else if (instructionType == 'withdraw') {
                  const deserialized = WithdrawLayout.decode(ix);
                  const lamports = Number(deserialized.lamports);
                  const uiAmount = lamports / LAMPORTS_PER_SOL
                  const from = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const to = data?.transaction.message.accountKeys[instruction.accounts[1]]
                  const withdrawAuthority = data?.transaction.message.accountKeys[instruction.accounts[4]]
                  const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},,${withdrawAuthority},,${(new PublicKey(from)).toBase58()},${(new PublicKey(to)).toString()},,,,${uiAmount}`;
                  console.log(message);
                  const encodedHash = hashMessage(message);
                  const fields = ['program', 'type', 'signature', 'err', 'slot', 'blocktime', 'fee', 'authority2', 'source', 'destination', 'uiAmount', 'serial']
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(withdrawAuthority)).toBase58()}'`,`'${(new PublicKey(from)).toBase58()}'`,`'${(new PublicKey(to)).toBase58()}'`,uiAmount,`'${encodedHash}'`];
                  return insertData(signature, fields, values);
              }
              else if (instructionType == 'merge') {
                const from = data?.transaction.message.accountKeys[instruction.accounts[1]] // source
                const to = data?.transaction.message.accountKeys[instruction.accounts[0]] // destination
                const stakeAuthority = data?.transaction.message.accountKeys[instruction.accounts[4]] // stake authority
                const uiAmount = data?.transaction.message.accountKeys.map((key, index) => {
                  if (key.toString() === to.toString()) {
                    const balanceChange = data?.meta.postBalances[index] - data?.meta.preBalances[index];
                    // lamports = data?.meta.postBalances[instruction.accounts[1]] - [instruction.accounts[1]]; // example from createAccount
                    // const uiAmount = lamports / LAMPORTS_PER_SOL
                      return balanceChange / LAMPORTS_PER_SOL;
                  }
                }).filter(Boolean)[0]; // Filter out undefined and take the first valid entry
                // add uiAmount based on final balance of the stake
                const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${stakeAuthority},,,${(new PublicKey(from)).toBase58()},${(new PublicKey(to)).toString()},,,,`;
                console.log(message);
                const encodedHash = hashMessage(message);
                const fields = ['program', 'type', 'signature', 'err', 'slot', 'blocktime', 'fee', 'authority', 'source', 'destination', 'uiAmount', 'serial']
                const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(stakeAuthority)).toBase58()}'`,`'${(new PublicKey(from)).toBase58()}'`,`'${(new PublicKey(to)).toBase58()}'`,uiAmount,`'${encodedHash}'`];
                return insertData(signature, fields, values);
              }
              else if (instructionType == 'split') {
                const deserialized = SplitLayout.decode(ix);
                const lamports = Number(deserialized.lamports);
                const uiAmount = lamports / LAMPORTS_PER_SOL
                const from = data?.transaction.message.accountKeys[instruction.accounts[0]] // source
                const to = data?.transaction.message.accountKeys[instruction.accounts[1]] // destination
                const stakeAuthority = data?.transaction.message.accountKeys[instruction.accounts[2]] // stake authority
                const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${stakeAuthority},,,${(new PublicKey(from)).toBase58()},${(new PublicKey(to)).toString()},,,,${uiAmount}`;
                console.log(message);
                const encodedHash = hashMessage(message);
                const fields = ['program', 'type', 'signature', 'err', 'slot', 'blocktime', 'fee', 'authority', 'source', 'destination', 'uiAmount', 'serial']
                const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(stakeAuthority)).toBase58()}'`,`'${(new PublicKey(from)).toBase58()}'`,`'${(new PublicKey(to)).toBase58()}'`,uiAmount,`'${encodedHash}'`];
                return insertData(signature, fields, values);
              }
              else if (instructionType === 'authorize') {
                // const deserialized = AuthorizeLayout.decode(ix);
                const authority3 = 'no custodian found' //(new PublicKey(deserialized.custodian)).toBase58();
                const source = data?.transaction.message.accountKeys[instruction.accounts[0]] // stakeAccount
                const authority = data?.transaction.message.accountKeys[instruction.accounts[2]] // old authority
                const authority2 = 'new authority not found' // new authority
                const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${(new PublicKey(authority)).toBase58()},${authority2},${authority3},${(new PublicKey(source)).toBase58()},,,,,`;
                console.log(message);
                const encodedHash = hashMessage(message);
                const fields = ['program', 'type', 'signature', 'err', 'slot', 'blocktime', 'fee', 'authority', 'authority2', 'authority3', 'source', 'serial']
                const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(authority)).toBase58()}'`,`'${authority2}'`,`'${authority3}'`,`'${(new PublicKey(source)).toBase58()}'`,`'${encodedHash}'`];
                return insertData(signature, fields, values);
              }
              else if (instructionType === 'authorizeChecked') {
                // const deserialized = AuthorizeCheckedLayout.decode(ix);
                // const authorizeWithCustodian = deserialized.authorizeWithCustodian;
                const source = data?.transaction.message.accountKeys[instruction.accounts[0]] // stakeAccount
                const authority = data?.transaction.message.accountKeys[instruction.accounts[2]] // old authority
                const authority2 = data?.transaction.message.accountKeys[instruction.accounts[3]] // new authority
                const authority3 = data?.transaction.message.accountKeys[instruction.accounts[4]] // custodian
                const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${(new PublicKey(authority)).toBase58()},${(new PublicKey(authority2)).toBase58()},${(new PublicKey(authority3)).toBase58()},${(new PublicKey(source)).toBase58()},,,,,`;
                console.log(message);
                const encodedHash = hashMessage(message);
                const fields = ['program', 'type', 'signature', 'err', 'slot', 'blocktime', 'fee', 'authority', 'authority2', 'authority3', 'source','serial']
                const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(authority)).toBase58()}'`,`'${(new PublicKey(authority2)).toBase58()}'`,`'${(new PublicKey(authority3)).toBase58()}'`,`'${(new PublicKey(source)).toBase58()}',${encodedHash}'`];
                return insertData(signature, fields, values);
              }
          } 
          else if (program == 'system'){
              if (instructionType == 'createAccount') {
                  const deserialized = CreateAccountLayout.decode(ix);
                  const lamports = Number(deserialized.lamports);
                  const uiAmount = lamports / LAMPORTS_PER_SOL
                  const from = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const to = data?.transaction.message.accountKeys[instruction.accounts[1]]
                  const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},,,,${from},${to},,,,${uiAmount}`;
                  console.log(message);
                  const encodedHash = hashMessage(message);
                  const fields = ['program','type','signature','err','slot','blocktime','fee','source','destination','uiAmount','serial'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(from)).toBase58()}'`,`'${(new PublicKey(to)).toBase58()}'`,uiAmount,`'${encodedHash}'`];
                  return insertData(signature, fields, values);
              }
              else if (instructionType == 'createAccountWithSeed') {
                  const deserialized = CreateAccountWithSeedLayout.decode(ix);
                  lamports = data?.meta.postBalances[instruction.accounts[1]] - data?.meta.preBalances[instruction.accounts[1]];
                  const uiAmount = lamports / LAMPORTS_PER_SOL
                  const space = Number(deserialized.space);
                  const seed = 'seed unavailable';
                  const from = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const to = data?.transaction.message.accountKeys[instruction.accounts[1]]
                  const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},,,,${from},${to},,${seed},,${uiAmount}`;
                  console.log(message);
                  const encodedHash = hashMessage(message);
                  const fields = ['program','type','signature','err','slot','blocktime','fee','source','destination','misc1','uiAmount','serial'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${from}'`,`'${to}'`,`'${seed}'`,`'${uiAmount}'`,`'${encodedHash}'`];
                  return insertData(signature, fields, values);

              }
              else if (instructionType == 'transfer') {
                  const deserialized = TransferLayout.decode(ix);
                  const lamports = Number(deserialized.lamports);
                  const uiAmount = lamports / LAMPORTS_PER_SOL
                  const from = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  const to = data?.transaction.message.accountKeys[instruction.accounts[1]]
                  const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},,,,${from},${to},,,,${uiAmount}`;
                  console.log(message);
                  const encodedHash = hashMessage(message);
                  const fields = ['program','type','signature','err','slot','blocktime','fee','source','destination','uiAmount','serial'];
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(from)).toBase58()}'`,`'${(new PublicKey(to)).toBase58()}'`,uiAmount,`'${encodedHash}'`];
                  return insertData(signature, fields, values);
              }
          } 
          else if (program == 'spl-token') {
              const prefix2 = ix.slice(0,1);
              const disc = (Buffer.from(prefix2)).readUInt8()
              const instructionType = MethodMap.get(`${program}_${disc}`)
              // const source = data?.transaction.message.accountKeys[instruction.accounts[0]]
              // const mint = data?.transaction.message.accountKeys[instruction.accounts[1]]
              // const destination = data?.transaction.message.accountKeys[instruction.accounts[2]]
              // const authority = data?.transaction.message.accountKeys[instruction.accounts[3]]
              if (instructionType == 'transfer') {
                  const source = data?.transaction.message.accountKeys[instruction.accounts[0]]
                  // const mint = data?.transaction.message.accountKeys[instruction.accounts[1]] // not available info?
                  const destination = data?.transaction.message.accountKeys[instruction.accounts[1]]
                  const authority = data?.transaction.message.accountKeys[instruction.accounts[2]]
                  const deserialized = TokenTransferLayout.decode(ix);
                  const amount = Number(deserialized.amount); // wrong
                  // const mint = "not available"
                  const decimals = 0 // Number(deserialized.decimals); // wrong
                  const uiAmount = amount / 10 ** decimals; // wrong
                  const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${authority},,,${source},,${destination},,${decimals},${uiAmount}`;
                  console.log(message);
                  const encodedHash = hashMessage(message);
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','source','destination','misc2','uiAmount','serial']
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(authority)).toBase58()}'`,`'${(new PublicKey(source)).toBase58()}'`,`'${(new PublicKey(destination)).toBase58()}'`,decimals,uiAmount,`'${encodedHash}'`];
                  return insertData(signature, fields, values);
              } 
              else if (instructionType == 'transferChecked') {
                const source = data?.transaction.message.accountKeys[instruction.accounts[0]]
                const mint = data?.transaction.message.accountKeys[instruction.accounts[1]]
                const destination = data?.transaction.message.accountKeys[instruction.accounts[2]]
                const authority = data?.transaction.message.accountKeys[instruction.accounts[3]]
                  const deserialized = TokenTransferCheckedLayout.decode(ix);
                  const amount = Number(deserialized.amount);
                  const decimals = Number(deserialized.decimals);
                  const uiAmount = amount / 10 ** decimals;
                  const message = `${program},${instructionType},${signature},${err},${slot},${blocktime},${fee},${authority},,,${source},${destination},,${mint},${decimals},${uiAmount}`;
                  console.log(message);
                  const encodedHash = hashMessage(message);
                  const fields = ['program','type','signature','err','slot','blocktime','fee','authority','source','destination','misc1','misc2','uiAmount','serial']
                  const values = [`'${program}'`,`'${instructionType}'`,`'${signature}'`,`'${err}'`,slot,blocktime,fee,`'${(new PublicKey(authority)).toBase58()}'`,`'${(new PublicKey(source)).toBase58()}'`,`'${(new PublicKey(destination)).toBase58()}'`,`'${(new PublicKey(mint)).toBase58()}'`,decimals,uiAmount,`'${encodedHash}'`];
                  return insertData(signature, fields, values);
              }
          }
          else {
              // console.log(`No result: ${signature} not a stake-related tx`);
          }
      });
  } catch (err) {
      console.log(err);
  }
  
    // Wait for all insertData promises to resolve and return the resulting promise
    return Promise.all(promises);

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