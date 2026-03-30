#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_statistics
Tape Management API - tape_statistics
"""

import logging
import traceback
import json
import re
import os
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends, BackgroundTasks
from pydantic import BaseModel

from .models import CreateTapeRequest, UpdateTapeRequest
from .tape_utils import normalize_tape_label, parse_expiry_date_for_inventory
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from utils.tape_tools import tape_tools_manager
from config.database import db_manager

logger = logging.getLogger(__name__)
router = APIRouter()




# Note: /inventory route is handled in tape_query.py to avoid conflicts
# The inventory statistics functionality has been moved to tape_query.py