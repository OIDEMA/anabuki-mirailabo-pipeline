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
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { createWriteStream, createReadStream } from "fs";
import fs from 'fs';
import { Readable } from "stream";
import { parse } from 'csv-parse/sync';
import moment from "moment";

/* kintoneのシークレット情報を取得する */
/* const secret_name = "anabuki-kubun-kintone"; */
const secret_name = "kintone-secret";

const client = new SecretsManagerClient({
  region: "ap-northeast-1",
});

/* メイン関数 */
export const handler = async () => {
  try {
    /* AWSからシークレット情報を取得する */
    const secret = await getAwsSecret();

    /* s3のクライアントを作成 */
    const s3Client = new S3Client({
      region: "ap-northeast-1",
      credentials: {
        accessKeyId: secret.awsAccessKey,
        secretAccessKey: secret.awsSecretKey,
      },
    });
    /* Kintoneからデータを取得する */
    const records = await fetchKintoneData(secret);

    await fetchS3Data(secret, records, s3Client);
    // console.log(records);
  } catch (error) {
    // For a list of exceptions thrown, see
    // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    // https://docs.aws.amazon.com/ja_jp/sdk-for-javascript/v3/developer-guide/javascript_s3_code_examples.html
    throw error;
  }
};

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
async function fetchS3Data(secret, records, s3Client) {

  const objectKey = "data-pipeline-test.csv";
  const command = new GetObjectCommand({
    Bucket: "mirai-property-master",
    Key: objectKey
  });

  try {
    const response = await s3Client.send(command);
    // const downloadPath = '/tmp/';
    const downloadPath = './';    
    const fileName = objectKey;
    const filePath = downloadPath + fileName;
    const body = response.Body;

    /* tmpに保管 */
    if (body instanceof Readable) {
      const writeStream = createWriteStream(filePath);
      body
        .pipe(writeStream)
        .on("error", (err) => console.log(err))
        .on("close", () => console.log("ダウンロード完了"))

        /* kintoneから取得したレコードをCSVに書き込む */
        let newRows = "";
        records.forEach((record) => {
          const newRow = setNewRecord(record);
          newRows += newRow;
          // console.log(newRow);
        })

        fs.writeFileSync(filePath, newRows, { flag: 'a' });
        /* fs.writeFileSync 関数の flag オプションに 'a' を指定すると、
          ファイル末尾に追記するようになります。既存のデータを上書きせずに追記したい場合は、このオプションを使用します。 
        */

        /* 書き込む用のデータを確認する */
        const csvData = fs.readFileSync(filePath);
        // const csvRecords = parse(csvData, { columns: true, delimiter: ',' });
        // for (const rec of csvRecords) {
        //   console.log(rec);
        // }

      /* 更新データをアップをアップロード */
      const updateCommand = new PutObjectCommand({
        Bucket: "mirai-property-master",
        Key: objectKey,
        Body: csvData,
      });
      await s3Client.send(updateCommand)
      return;
    }

  } catch (err) {
    console.log(err);
    return err;
  }
}

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

/* アップロード対象のデータを*/
function setNewRecord(record) {

  const created_date = formatDate(record["作成日時"].value);
  const updated_date = formatDate(record["更新日時"].value);

  const newRecord = {
    "レコード番号": record["レコード番号"].value,
    name: record.name.value,
    number: record.number.value,
    "更新者": record["更新者"].value.name,
    "作成者": record["作成者"].value.name,
    "更新日時": updated_date,
    "作成日時": created_date
  }
  const csvString = `"${newRecord["レコード番号"]}","${newRecord.name}","${newRecord.number}","${newRecord["更新者"]}","${newRecord["作成者"]}","${newRecord["更新日時"]}","${newRecord["作成日時"]}"\n`;
  // const csvString = `${newRecord["レコード番号"]},${newRecord.name},${newRecord.number},${newRecord["更新者"]},${newRecord["作成者"]},${newRecord["更新日時"]},${newRecord["作成日時"]}`;
  return csvString;
}

/* データ型をkintoneに合わせる */
function formatDate(dateString) {
  // Moment.jsを使用して日付をパース
  const parsedDate = moment(dateString);
  // フォーマットされた日付文字列を返す
  return parsedDate.format('YYYY/MM/DD hh:mm');
}

handler();
