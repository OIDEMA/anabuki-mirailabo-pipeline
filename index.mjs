/*
1. AWS Secrets Managerからシークレット情報を取得
2. AWS S3からExcelファイルを取得しtmpに保管
3. Exceljsでファイルを値によって編集
*/

/* データ取得について

1. created_dateが当日（TODAY）のデータをCSVに追記する
2. updated_dateが当日（TODAY）かつ、created_dateがupdated_dateより小さいレコードは更新
3. データが削除された場合、削除された場合にマスタデータを削除しにいく、Functionを動作させる
4. deleteされたレコードはKintone Procy経由でS3データを削除しにいく

*/

import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { KintoneRestAPIClient } from "@kintone/rest-api-client";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { createWriteStream, createReadStream } from "fs";
import fs from 'fs';
import { Readable } from "stream";
import { parse } from 'csv-parse/sync';

/* kintoneのシークレット情報を取得する */
/* const secret_name = "anabuki-kubun-kintone"; */
const secret_name = "kintone-secret";

const client = new SecretsManagerClient({
  region: "ap-northeast-1",
});

/* メイン関数 */
export const handler = async () => {
  try {
    const secret = await getAwsSecret();
    // console.log(secret);
    // const records = await fetchKintoneData(secret);
    // console.log(records);
    await fetchS3Data(secret);
    // console.log(records);
  } catch (error) {
    // For a list of exceptions thrown, see
    // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    // https://docs.aws.amazon.com/ja_jp/sdk-for-javascript/v3/developer-guide/javascript_s3_code_examples.html
    throw error;
  }
};


/* AWS Secret Managerからシークレット情報を取得する */
async function getAwsSecret() {
  const res = await client.send(
    new GetSecretValueCommand({
      SecretId: secret_name,
    })
  );
  const secret = JSON.parse(res.SecretString);
  return secret;
}

/* Kintoneからデータを取得する */
async function fetchKintoneData(secret) {
  const params = {
    app: 409,
    /* https://cybozu.dev/ja/kintone/docs/overview/query/ */
    /* query: '物件ステータス in ("予定物件（1年以内）")' */
    query: '作成日時 >= TODAY() and 更新日時 >= TODAY()'
    /* query: '作成日時 > YESTERDAY()' */
  }
  const client = new KintoneRestAPIClient({
    baseUrl: secret.baseUrl,
    auth: {
      username: secret.kintoneUser,
      password: secret.kintonePass
    },
    basicAuth: { // Basic 認証の設定
      username: secret.basicAuthUser,
      password: secret.basicAuthPass
    },
  })
  const result = await client.record.getRecords(params);
  return result.records
}

/* S3データを読みとる */
async function fetchS3Data(secret) {
  // AWS 認証情報の設定
  const s3Client = new S3Client({
    region: "ap-northeast-1",
    credentials: {
      accessKeyId: secret.awsAccessKey,
      secretAccessKey: secret.awsSecretKey,
    },
  });

  const objectKey = "data-pipeline-test.csv";
  const command = new GetObjectCommand({
    Bucket: "mirai-property-master",
    Key: objectKey
  });

  try {
    const response = await s3Client.send(command);
    const downloadPath = '/tmp/';
    const fileName = objectKey;
    const filePath = downloadPath + fileName;

    const body = response.Body;

    /* tmpに保管 */
    if (body instanceof Readable) {
      const writeStream = createWriteStream(filePath);
      body
        .pipe(writeStream)
        .on("error", (err) => console.log(err))
        .on("close", () => console.log(null));
    }

    /* tmpにダウンロードしたデータを読み取り */
    const csvData = fs.readFileSync(filePath);
    // console.log(csvData);

    const records = parse(csvData, { columns: true, delimiter: ',' });
    for (const record of records) {
      console.log(record);
    }
    return;

  }catch(err){
    console.log(err);
    return err;
  }
}


/* 作成日、更新日データの変換 */
const formatDate = (dateString) => {
  return moment(dateString).format('YYYY-MM-DD');
}


handler();
