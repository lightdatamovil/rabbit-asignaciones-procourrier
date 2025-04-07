import { connect } from 'amqplib';
import dotenv from 'dotenv';
import { desasignar } from './controller/desasignacion/desasignacion.js';
import { verifyParamaters } from './src/funciones/verifyParameters.js';
import { getCompanyById, redisClient } from './db.js';
import { logBlue, logGreen, logPurple, logRed, logYellow } from './src/funciones/logsCustom.js';
import { verificacionDeAsignacion } from './controller/asignacion/verificarAsignacion.js';

dotenv.config({ path: process.env.ENV_FILE || '.env' });

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME_ASIGNACION = process.env.QUEUE_NAME_ASIGNACION;
const QUEUE_NAME_DESASIGNACION = process.env.QUEUE_NAME_DESASIGNACION;

async function connectRabbitMQ() {
    try {
        await redisClient.connect();
        const connection = await connect(RABBITMQ_URL);

        const channel = await connection.createChannel();
        await channel.assertQueue(QUEUE_NAME_ASIGNACION, { durable: true });
        await channel.assertQueue(QUEUE_NAME_DESASIGNACION, { durable: true });

        logBlue(`[*] Esperando mensajes en la cola "${QUEUE_NAME_ASIGNACION}"`);

        logBlue(`[*] Esperando mensajes en la cola "${QUEUE_NAME_DESASIGNACION}"`);

        channel.consume(QUEUE_NAME_ASIGNACION, async (msg) => {
            const startTime = performance.now();
            if (msg !== null) {
                const body = JSON.parse(msg.content.toString());
                try {
                    logGreen(`Mensaje recibido: ${JSON.stringify(body)}`);

                    const errorMessage = verifyParamaters(body, ['dataQr', 'driverId', 'deviceFrom', 'channel'], true);

                    if (errorMessage) {
                        logRed(`Error al verificar los parámetros: ${errorMessage}`);
                        return { mensaje: errorMessage };
                    }

                    const company = await getCompanyById(body.companyId);

                    const resultado = await verificacionDeAsignacion(startTime, company, body.userId, body.profile, body, body.driverId, body.deviceFrom);

                    resultado.feature = 'asignacion';
                    channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify(resultado)),
                        { persistent: true }
                    );

                    logGreen(`Respuesta enviada al canal ${body.channel}: ${JSON.stringify(resultado)}`);

                } catch (error) {
                    logRed(`Error al procesar el mensaje: ${error.stack}`);
                    let a = channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify({ feature: 'asignacion', estadoRespuesta: false, mensaje: error.stack, error: true })),
                        { persistent: true }
                    );
                    if (a) {
                        logRed(`Mensaje enviado al canal ${body.channel}: { feature: ${body.feature}, estadoRespuesta: false, mensaje: ${error.stack}, error: true }`);
                    }
                } finally {
                    channel.ack(msg);
                    const endTime = performance.now();
                    logPurple(`Tiempo de ejecución: ${endTime - startTime} ms`);
                }
            }
        });
        channel.consume(QUEUE_NAME_DESASIGNACION, async (msg) => {
            const startTime = performance.now();
            if (msg !== null) {
                const body = JSON.parse(msg.content.toString());
                try {
                    logGreen(`Mensaje recibido: ${JSON.stringify(body)}`);

                    const errorMessage = verifyParamaters(body, ['dataQr', 'deviceFrom', 'channel'], true);

                    if (errorMessage) {
                        logRed(`Error al verificar los parámetros: ${errorMessage}`);
                        return { mensaje: errorMessage };
                    }

                    const company = await getCompanyById(body.companyId);

                    const resultado = await desasignar(startTime, company, body.userId, body, body.deviceFrom);

                    resultado.feature = 'asignacion';

                    channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify(resultado)),
                        { persistent: true }
                    );
                    logGreen(`Respuesta enviada al canal ${body.channel}: ${JSON.stringify(resultado)}`);

                } catch (error) {
                    logRed(`Error al procesar el mensaje: ${error.stack}`);
                    let a = channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify({ feature: 'asignacion', estadoRespuesta: false, mensaje: error.stack, error: true })),
                        { persistent: true }
                    );
                    if (a) {
                        logGreen("Mensaje enviado al canal", body.channel + ":", { feature: body.feature, estadoRespuesta: false, mensaje: error.stack });
                    }
                } finally {
                    const endTime = performance.now();
                    logPurple(`Tiempo de ejecución: ${endTime - startTime} ms`);
                    channel.ack(msg);
                }
            }
        });
    } catch (error) {
        logRed(`Error al conectar con Redis: ${error.stack}`);
    }
}

connectRabbitMQ();
