const mysql = require("mysql2")
require("dotenv").config()
const moment = require("moment")

const {Client} = require('ssh2');

const conn = new Client();




const path = require("path")

const csv = require("fast-csv")


const connection = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME
})


const yesterday = moment().subtract(1, "day").format("YYYY-MM-DD")

const outputFile = path.join(__dirname, "out", `${yesterday}.csv`)

const outputFileRemote = `${process.env.FTP_REMOTE_DIR}/${yesterday}.csv`


const sql = `select r.suuid, r.msisdn, r.createdAt, r.customer_type,  r.originalPayload, g.niaData
from registeredMsisdn r
INNER JOIN ghanaIds g
on r.cardNumber = g.pinNumber
where r.createdAt like '${yesterday}%'`


connection.query(sql, (err, result) => {
    if (err) {
        console.log(err)

    } else {
        if (result.length > 0) {
            let final_data = []
            for (const data of result) {
                const {
                    suuid,
                    msisdn,
                    createdAt: registration_date,
                    customer_type: registration_type,
                    originalPayload,
                    niaData
                } = data
                const customer_data = JSON.parse(originalPayload)
                const nia_data = JSON.parse(niaData)
                const {
                    first_name,
                    last_name: surname,
                    national_Id_type: id_type,
                    ghana_card_number: id_number,
                    nationality,
                    digital_address,
                    dob,
                    gender
                } = customer_data
                const {
                    person: {
                        cardValidTo: dateOfExpiry,
                        digitalAddress: {digitalAddress: NIADigitalAddress}
                    }
                } = nia_data

                final_data.push({
                    entity: "Individual",
                    businessEntity: null,
                    businessCertificate: null,
                    first_name, surname, dob, id_type, id_number, dateOfExpiry, nationality, gender, NIADigitalAddress,
                    digital_address, imsi: null, msisdn, suuid, registration_date:moment(registration_date).format(),bcap_date:moment(registration_date).format(),UUID:null,biometricVerification:null,registration_type
                })

            }


            csv.writeToPath(outputFile, final_data, {headers: true})

            conn.on('ready', () => {
                console.log('Client :: ready');
                conn.sftp((err, sftp) => {
                    if (err) throw err;

                    sftp.fastPut(outputFile, outputFileRemote, err => {
                        if (err) throw  err
                        conn.end()
                    })


                });
            }).connect({
                host: process.env.FTP_HOST,
                port: process.env.FTP_PORT,
                username: process.env.FTP_USER,
                password: process.env.FTP_PASS
            });




        }


    }

    connection.end(err => {
        if (err) console.log(err)


    })

})








