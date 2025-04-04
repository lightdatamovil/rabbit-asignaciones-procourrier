import { connect } from 'amqplib';
import dotenv from 'dotenv';
import { desasignar, verificacionDeAsignacion } from './controller/asignacionesController.js';
import { verifyParamaters } from './src/funciones/verifyParameters.js';
import { getCompanyById, redisClient } from './db.js';

dotenv.config({ path: process.env.ENV_FILE || '.env' });

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME_ASIGNACION = process.env.QUEUE_NAME_ASIGNACION;
const QUEUE_NAME_DESASIGNACION = process.env.QUEUE_NAME_DESASIGNACION;

async function startConsumer() {
    await redisClient.connect();
    let connection;
    let channel;

    const connectWithRetry = async () => {
        try {
            connection = await connect(RABBITMQ_URL);
            channel = await connection.createChannel();

            await channel.assertQueue(QUEUE_NAME_ASIGNACION, { durable: true });
            await channel.assertQueue(QUEUE_NAME_DESASIGNACION, { durable: true });

            console.log(`[*] Esperando mensajes en "${QUEUE_NAME_ASIGNACION}" y "${QUEUE_NAME_DESASIGNACION}"`);

            connection.on('close', () => {
                console.error('[!] Conexión cerrada. Reintentando...');
                return reconnect();
            });

            connection.on('error', (err) => {
                console.error('[!] Error en la conexión:', err.message);
            });

            channel.consume(QUEUE_NAME_ASIGNACION, async (msg) => {
                if (!msg) return;
                const body = JSON.parse(msg.content.toString());

                try {
                    console.log("[x] Mensaje recibido:", body);

                    const errorMessage = verifyParamaters(body, ['dataQr', 'driverId', 'deviceFrom', 'channel']);
                    if (errorMessage) {
                        console.log("[x] Error al verificar los parámetros:", errorMessage);
                        return;
                    }

                    const company = await getCompanyById(body.companyId);
                    const resultado = await verificacionDeAsignacion(company, body.userId, body.profile, body.dataQr, body.driverId, body.deviceFrom);

                    const nowHour = new Date().toLocaleTimeString();
                    const startSendTime = performance.now();

                    channel.sendToQueue(body.channel, Buffer.from(JSON.stringify(resultado)), { persistent: true });

                    const sendDuration = performance.now() - startSendTime;

                    console.log(`[x] Respuesta enviada al canal ${body.channel} a las ${nowHour}:`, resultado);
                    console.log(`Tiempo de envío: ${sendDuration.toFixed(2)} ms`);
                } catch (error) {
                    console.error("[x] Error al procesar el mensaje:", error);
                    channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify({
                            feature: body.feature,
                            estadoRespuesta: false,
                            mensaje: error.message,
                            error: true
                        })),
                        { persistent: true }
                    );
                } finally {
                    channel.ack(msg);
                }
            });

            channel.consume(QUEUE_NAME_DESASIGNACION, async (msg) => {
                if (!msg) return;
                const body = JSON.parse(msg.content.toString());

                try {
                    console.log("[x] Mensaje recibido:", body);

                    const errorMessage = verifyParamaters(body, ['dataQr', 'driverId', 'deviceFrom', 'channel']);
                    if (errorMessage) {
                        console.log("[x] Error al verificar los parámetros:", errorMessage);
                        return;
                    }

                    const company = await getCompanyById(body.companyId);
                    const resultado = await desasignar(company, body.userId, body.dataQr, body.driverId, body.deviceFrom);

                    const nowHour = new Date().toLocaleTimeString();
                    const startSendTime = performance.now();

                    channel.sendToQueue(body.channel, Buffer.from(JSON.stringify(resultado)), { persistent: true });

                    const sendDuration = performance.now() - startSendTime;

                    console.log(`[x] Respuesta enviada al canal ${body.channel} a las ${nowHour}:`, resultado);
                    console.log(`Tiempo de envío: ${sendDuration.toFixed(2)} ms`);
                } catch (error) {
                    console.error("[x] Error al procesar el mensaje:", error);
                    channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify({
                            feature: body.feature,
                            estadoRespuesta: false,
                            mensaje: error.message,
                            error: true
                        })),
                        { persistent: true }
                    );
                } finally {
                    channel.ack(msg);
                }
            });

        } catch (error) {
            console.error('[!] Error conectando a RabbitMQ:', error.message);
            await reconnect();
        }
    };

    const reconnect = async () => {
        setTimeout(() => {
            console.log('[~] Intentando reconectar...');
            connectWithRetry();
        }, 5000); // 5 segundos de delay entre reintentos
    };

    await connectWithRetry();
}

startConsumer();
