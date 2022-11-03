from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import os
import asyncio


class Concurrency:

    def __init__(self, worker_thread=os.cpu_count()+4, worker_process=os.cpu_count(), chunk_size=1):
        self.worker_thread = worker_thread
        self.worker_process = worker_process
        self.chunk_size = chunk_size

    def get_param_chunks(self, param_list):
        chunk_size = 10 if self.chunk_size else self.chunk_size
        params = [param_list[i:i+chunk_size]
                  for i in range(0, len(param_list), chunk_size)]
        return params

    def run_process(self, func=None, param_list=[], param_name=None, is_threaded=False, cb_func=None):
        params = self.get_param_chunks(param_list)
        futures = []
        with ProcessPoolExecutor(max_workers=self.worker_process) as pexecutor:
            for param in params:
                if is_threaded:
                    future = pexecutor.submit(
                        self.run_thread, func=func, param_list=param, param_name=param_name, cb_func=cb_func)
                else:
                    future = pexecutor.submit(
                        self.run_async, func=func, param_list=param, param_name=param_name, cb_func=cb_func)
                futures.append(future)
            for future in as_completed(futures):
                self.handler(future.result(), is_process=True, cb_func=cb_func)

    def run_thread(self, func=None, param_list=[], param_name=None, cb_func=None):
        func_name, params = self.initialize(func, param_list, param_name)
        with ThreadPoolExecutor(max_workers=self.worker_thread) as executor:
            futures = self.submit(executor, func_name, params)
            for future in as_completed(futures):
                self.handler(future.result(), cb_func=cb_func)

    async def run_async_tasks(self, func, params, cb_func=None):
        tasks = []
        for param in params:
            task = asyncio.create_task(func(**param))
            tasks.append(task)

        for future in asyncio.as_completed(tasks):
            self.handler(await future, cb_func=cb_func)

    def run_async(self, func=None, param_list=[], param_name=None, cb_func=None):
        func_name, params = self.initialize(func, param_list, param_name)
        asyncio.run(self.run_async_tasks(func_name, params, cb_func))

    def submit(self, executor, func_name, params):
        futures = []
        for param in params:
            exec = executor.submit(func_name, **param)
            futures.append(exec)
        return futures

    def initialize(self, func, param_list, param_name=None):
        func_name = func
        params = []
        if not param_name:
            params = param_list
        else:
            param_obj_list = []
            for param in param_list:
                param_obj = {key: value for key,
                             value in zip(param_name, param)}
                #print(param_obj)
                param_obj_list.append(param_obj)
            params = param_obj_list
        return func_name, params

    def handler(self, response, is_process=False, cb_func=None):
        if not is_process and cb_func is not None:
            cb_func(response)
