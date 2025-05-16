# rpc.py
import asyncio
import json
import logging
import time
from enum import Enum
from typing import Dict, Any, Optional

from config import RPC_BUFFER_SIZE, RPC_MESSAGE_LENGTH_BYTES

logger = logging.getLogger(__name__)

class MessageType(Enum):
    # Client <-> Proxy
    CLIENT_REQUEST = "CLIENT_REQUEST"
    CLIENT_RESPONSE = "CLIENT_RESPONSE"
    # Proxy <-> Worker (Control Plane via Proxy's Internal RPC Server)
    REGISTER_WORKER = "REGISTER_WORKER" # Worker -> Proxy
    REGISTER_ACK = "REGISTER_ACK"       # Proxy -> Worker
    METRICS_UPDATE = "METRICS_UPDATE"   # Worker -> Proxy
    # Proxy <-> Worker (Data Plane - requests routed by proxy to worker's listening port)
    # The worker connects to proxy's internal RPC for control.
    # Proxy connects to worker's RPC server for data.
    # OR, worker's connection to proxy's internal RPC is used bi-directionally.
    # The current implementation uses the worker's connection to proxy for bi-directional comms.
    WORKER_REQUEST = "WORKER_REQUEST"   # Proxy -> Worker
    WORKER_RESPONSE = "WORKER_RESPONSE" # Worker -> Proxy
    # Error
    ERROR = "ERROR"

async def send_message(writer: asyncio.StreamWriter, message_type: MessageType, payload: Dict[str, Any]):
    message = {
        "type": message_type.value,
        "payload": payload,
        "timestamp": time.time()
    }
    try:
        json_message = json.dumps(message).encode('utf-8')
        message_len = len(json_message)
        writer.write(message_len.to_bytes(RPC_MESSAGE_LENGTH_BYTES, 'big'))
        writer.write(json_message)
        await writer.drain()
    except (ConnectionResetError, BrokenPipeError) as e:
        # logger.warning(f"Connection error while sending to {writer.get_extra_info('peername')}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error sending message: {e}", exc_info=True)
        raise

async def read_message(reader: asyncio.StreamReader) -> Optional[Dict[str, Any]]:
    try:
        len_bytes = await reader.readexactly(RPC_MESSAGE_LENGTH_BYTES)
        if not len_bytes:
            return None
        message_len = int.from_bytes(len_bytes, 'big')

        json_message = await reader.readexactly(message_len)
        if not json_message:
            return None

        message = json.loads(json_message.decode('utf-8'))
        return message
    except asyncio.IncompleteReadError:
        # logger.warning(f"Incomplete read from {reader._transport.get_extra_info('peername') if reader._transport else 'unknown'}. Connection likely closed.")
        return None
    except (ConnectionResetError, BrokenPipeError) as e:
        # logger.warning(f"Connection error while reading: {e}")
        return None
    except Exception as e:
        logger.error(f"Error reading message: {e}", exc_info=True)
        return None