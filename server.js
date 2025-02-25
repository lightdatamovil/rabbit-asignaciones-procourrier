import express, { json, urlencoded } from 'express';
import asignaciones from './routes/asignaciones.js';
import { redisClient } from './db.js';

const app = express();

app.use(json({ limit: '50mb' }));
app.use(urlencoded({ limit: '50mb', extended: true }));
app.use(json());

const PORT = process.env.PORT || 3000;

app.use("/api/asignaciones", asignaciones)

await redisClient.connect();

app.listen(PORT, () => {
    console.log(`Servidor corriendo en el puerto ${PORT}`);
});
