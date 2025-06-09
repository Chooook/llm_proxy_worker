import os
import subprocess
from contextlib import asynccontextmanager
from urllib.parse import quote_plus

import asyncio_atexit
import asyncpg
from asyncpg import Pool
from loguru import logger

from settings import settings

USERNAME = os.getenv('USERNAME_GP')
PASSWORD = quote_plus(os.getenv('PASSWORD_GP', ''))
HOST = settings.GP_HOST
PORT = settings.GP_PORT
DATABASE = settings.GP_DATABASE
SCHEMA = settings.GP_SCHEMA

pool: Pool | None = None


async def set_task_to_query(task:str):
    set_task_query = f'SELECT {SCHEMA}.f_ask_quest($1);'
    gp_task_id = await run_query(set_task_query, (task,))
    return gp_task_id[0][0]


async def get_task_result(gp_task_id: int):
    get_answer_query = f'SELECT {SCHEMA}.f_get_answer_by_id($1);'
    not_valid_result = ['Вопрос пользователя', 'еще не обработан']
    result = await run_query(get_answer_query, (gp_task_id,))
    if any(phrase in result[0][0].strip() for phrase in not_valid_result):
        result = None
    else:
        result = result[0][0].strip()
    return result


async def run_query(query, params=()):
    async with get_pg_conn() as conn:
        result = await conn.fetch(query, *params)
        return result


@asynccontextmanager
async def get_pg_conn():
    global pool
    if pool is None:
        try:
            await __init_db()
        except Exception as e:
            logger.error(f"⚠️ Ошибка при инициализации PG соединения: {e}")
            raise

    async with pool.acquire() as conn:
        yield conn


async def __init_db():
    __check_kerberos_ticket()
    global pool
    if pool is not None:
        return
    pool = await asyncpg.create_pool(
        dsn=f'postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/'
            f'{DATABASE}?client_encoding=utf8',
        min_size=1,
        max_size=5,
        max_inactive_connection_lifetime=30)
    logger.info("✅ Connection pool GP создан")
    __register_cleanup_handlers()


def __register_cleanup_handlers():
    asyncio_atexit.register(__close_db)


async def __close_db():
    global pool
    if pool:
        await pool.close()
        logger.info("✅ Connection pool закрыт")
        pool = None


def __check_kerberos_ticket():
    # needs installed krb5 and asyncgp[gssauth]
    try:
        subprocess.run(['klist'], check=True)
    except subprocess.CalledProcessError:
        raise RuntimeError('⚠️ Kerberos ticket not found or expired')
