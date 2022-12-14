import logging
import os
import socketserver
from socketserver import StreamRequestHandler, TCPServer
from typing import Optional

from stem.envelope import Envelope
from stem.task_master import TaskMaster, TaskStatus
from stem.task_runner import SimpleRunner
from stem.task_tree import TaskTree
from stem.workspace import IWorkspace
from multiprocessing import Process

import json
from stem_framework.stem.meta import get_meta_attr
from threading import Thread
class UnitHandler(StreamRequestHandler):
    workspace: IWorkspace
    task_tree: TaskTree = TaskTree()
    task_master = TaskMaster(SimpleRunner(), task_tree)
    powerfullity: int

    def handle(self) -> None:
        global response
        self.envelope = Envelope.read(self.rfile)
        # logging.info(f"Command received: {get_meta_attr(self.envelope.meta, 'command')}")

        if get_meta_attr(self.envelope.meta, 'command') == 'run':
            task_path = get_meta_attr(self.envelope.meta, 'task_path')
            task = self.workspace.find_task(task_path)
            if task is None:
                response = Envelope({'status': 'failed', 'error': 'Task not found'})
            else:
                response = self.task_master.execute(get_meta_attr(self.envelope.meta, 'task_meta', {}), task, self.workspace)
                if response.status == TaskStatus.CONTAINS_DATA:
                    data = response.lazy_data()
                    response = Envelope({'status': 'filled'}, json.dumps(data).encode('utf8'))
                else:
                    response = Envelope({'status': 'failed', 'error': str(response.status)})

        elif get_meta_attr(self.envelope.meta, 'command') == 'structure':
            structure = self.workspace.structure()
            response = Envelope({'status': 'fulfilled'}, json.dumps(structure).encode('utf8'))

        elif get_meta_attr(self.envelope.meta, 'command') == 'powerfullity':
            response = Envelope({'status': 'filled', 'powerfullity': self.powerfullity})

        else:
            response = Envelope({'status': 'failed', 'error': 'command is unknown'})

        logging.info(f'Server response: {response.meta}')
        self.wfile.write(response.to_bytes())



def start_unit(workspace: IWorkspace, host: str, port: int, powerfullity: Optional[int] = None):
    UnitHandler.workspace = workspace
    UnitHandler.task_master = TaskMaster()
    UnitHandler.task_tree = None
    UnitHandler.powerfullity = powerfullity
    return TCPServer((host, port), UnitHandler)


def start_unit_in_subprocess(workspace: IWorkspace, host: str, port: int, powerfullity: Optional[int] = None) -> Process:
    server = start_unit(workspace, host, port, powerfullity)
    thread = Thread(target=server.serve_forever)
    thread.start()
    logging.debug('thread started')
    return thread, server
