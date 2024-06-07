import { SecretsManagerClient, GetSecretValueCommand } from "@aws-sdk/client-secrets-manager";
import { KintoneRestAPIClient } from "@kintone/rest-api-client";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { createWriteStream, createReadStream } from "fs";
import fs from 'fs';
import { Readable } from "stream";
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import moment from "moment";

/* kintoneのシークレット情報を取得する */
/* const secret_name = "anabuki-kubun-kintone"; */
const secret_name = "kintone-secret";

const client = new SecretsManagerClient({
    region: "ap-northeast-1",
});

const objectKey = "data-pipeline-test.csv";
// const downloadPath = '/tmp/';
const downloadPath = './';
const filePath = downloadPath + objectKey;


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
        // console.log(records);

        /* S3からファイルのダウンロード */
        await fetchS3file(secret, s3Client, records);

        /* 編集してデータをアップロード */
        // await uploadS3(records, s3Client);

    } catch (error) {
        // For a list of exceptions thrown, see
        // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        // https://docs.aws.amazon.com/ja_jp/sdk-for-javascript/v3/developer-guide/javascript_s3_code_examples.html
        throw error;
    }
};

/* Kintoneからデータを取得する */
async function fetchKintoneData(secret) {
    console.log("Kintoneデータ取得開始");
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
async function fetchS3file(secret, s3Client, records) {
    console.log("S3データ取得開始");
    const command = new GetObjectCommand({
        Bucket: "mirai-property-master",
        Key: objectKey
    });

    try {
        const response = await s3Client.send(command);
        const body = response.Body;

        /* tmpに保管 */
        if (body instanceof Readable) {
            const writeStream = createWriteStream(filePath);
            /* body はReadable Stream */
            body
                .pipe(writeStream)
                .on('error', (err) => console.log(err))
                // .on('close', () => console.log("ダウンロード完了"))
                .on('close', () => uploadS3(records, s3Client))
        } else {
            console.log("Readableではありません。");
        }
    } catch (err) {
        console.log(err);
        return err;
    }
}

// async function uploadS3(records, s3Client) {
//     console.log("S3へのデータアップロード開始");
//     /* CSVファイルを読み込む */
//     const downloadData = fs.readFileSync(filePath);

//     // 先頭行をcolumnとして扱い，csvデータをparseして配列オブジェクト化
//     const parsedData = parse(downloadData, { columns: true });
//     // console.log({"parsedData1": parsedData});

//     /* kintoneから取得したレコードを配列(CSV)に書き込む */
//     records.forEach((record) => {

//         const created_date = formatDate(record["作成日時"].value);
//         const updated_date = formatDate(record["更新日時"].value);

//         parsedData.push({
//             "レコード番号": record["レコード番号"].value,
//             name: record.name.value,
//             number: record.number.value,
//             "更新者": record["更新者"].value.name,
//             "作成者": record["作成者"].value.name,
//             "更新日時": updated_date,
//             "作成日時": created_date
//         })
//     })

//     /* 配列データを文字化 */
//     const csvString = stringify(parsedData, { header: true });
//     // console.log(csvString);

//     /* 文字化したデータを書き込むためのストリームを作成 */
//     const writableStream = fs.createWriteStream(filePath);

//     /* 作成したストリームに文字化したデータを書き込み */
//     writableStream.write(csvString);

//     /* 書き込んだデータを読み込みストリームで読み込み。 */
//     const csvData = fs.readFileSync(filePath, { encoding: 'utf-8' });

//     // console.log(csvData);

//     /* 更新データをアップをアップロード */
//     const updateCommand = new PutObjectCommand({
//         Bucket: "mirai-property-master",
//         Key: objectKey,
//         Body: csvData,
//     });
//     await s3Client.send(updateCommand);
// }

async function uploadS3(records, s3Client) {
    console.log("S3へのデータアップロード開始");

    // CSVファイルを読み込む
    const csvString = fs.readFileSync(filePath, 'utf-8');

    // CSVデータをparseして配列オブジェクト化
    const parsedData = await parse(csvString, { columns: true });

    // kintoneから取得したレコードを配列(CSV)に書き込む
    records.forEach((record) => {
        const formattedCreatedDate = formatDate(record["作成日時"].value);
        const formattedUpdatedDate = formatDate(record["更新日時"].value);

        parsedData.push({
            "レコード番号": record["レコード番号"].value,
            name: record.name.value,
            number: record.number.value,
            "更新者": record["更新者"].value.name,
            "作成者": record["作成者"].value.name,
            "更新日時": formattedUpdatedDate,
            "作成日時": formattedCreatedDate
        })
    })

    // 文字化したデータを書き込むためのストリームを作成
    const writableStream = fs.createWriteStream(filePath);

    // 作成したストリームに文字化したデータを書き込み
    // console.log({"parsedData": parsedData});
    await writableStream.write(stringify(parsedData, { header: true }));
    await writableStream.close(() =>{
        // アップロードデータをアップロード
        const updateCommand = new PutObjectCommand({
            Bucket: "mirai-property-master",
            Key: objectKey,
            Body: fs.readFileSync(filePath)
        });
        s3Client.send(updateCommand);

        console.log("S3へのデータアップロード完了");
    });

}


/* AWS Secret Managerからシークレット情報を取得する */
async function getAwsSecret() {
    console.log("シークレット取得開始");
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
