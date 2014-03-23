# Copyright (c) 2012-2014 Waldemar Augustyn. All rights reserved.

# Simple CPU bench mark test.
#
# It can be run stand alone on a command line:
#
#   python qmark.py
#
# or it can be invoked from a python program:
#
#   from qmark import qmark
#   result = qmark()

import gevent
from gevent.queue import Queue
import time

__all__ = ["qmark"]

QTASKS = 61
CTASKS = 379
RUNS = 7

class QMark:

    def __init__(self, num_qtasks, num_ctasks, debug = False):
        self.num_qtasks = num_qtasks
        self.num_ctasks = num_ctasks
        self.debug = debug

    def run(self):
        '''Run the benchmark
        '''
        start_time = time.time()
        self.qtask_queues = [Queue() for _ in range(self.num_qtasks)]
        self.ctask_queues = [Queue() for _ in range(self.num_ctasks)]
        qglets = [gevent.spawn(self.qtask, ix) for ix in range(self.num_qtasks)]
        cglets = [gevent.spawn(self.ctask, ix) for ix in range(self.num_ctasks)]
        gevent.joinall(cglets)
        for qq in self.qtask_queues:
            qq.put('exit::')
        gevent.joinall(qglets)
        result = time.time() - start_time
        return result

    def build_id(self, name, idx):
        ''' Construct an id out of name and index
        '''
        return name + '(' + str(idx) + ')'

    def parse_id(self, tid):
        ''' Parse task or queue id into name and index
        '''
        name = tid[:tid.index('(')]
        idx = str(tid[len(name) + 1: -1])
        return name, int(idx)

    def qtask(self, qtid):
        ''' Qmark micro server

            message format:

                CMD:QIDX:PARAMS

                CMD     - command:
                            exit:   - exit
                            queue   - put on queue QIDX and add its index to params

                QIDX    - index of the queue to put on: cq(ix) or qq(ix)

                PARAMS  - parameters: ctask(ix) qtask(ix)
        '''

        for msg in self.qtask_queues[qtid]:
            cmd, qidx, params = msg.split(':')
            if self.debug:
                print('qtask({0}): {1}'.format(qtid, msg))
            if cmd == 'exit':
                break
            name, idx = self.parse_id(qidx)
            new_params = params.split('-') + [self.build_id('qtask', qtid)]
            if name == 'cq':
                new_msg = ':'.join(['queue', self.build_id('ct', idx),'-'.join(new_params)])
                self.ctask_queues[idx].put(new_msg)
            gevent.sleep(0)


    def ctask(self, ctid):
        ''' Qmark micro client
        '''
        count = self.num_qtasks
        dst_ix = divmod(ctid, self.num_qtasks)[1]
        new_msg = ':'.join(['queue', self.build_id('cq', ctid), self.build_id('ctask', ctid)])
        self.qtask_queues[dst_ix].put(new_msg)
        for msg in self.ctask_queues[ctid]:
            cmd, qidx, params = msg.split(':')
            if self.debug:
                print('ctask({0}): {1}'.format(ctid, msg))
            dst_ix = divmod(dst_ix + 1, self.num_qtasks)[1]
            new_params = params.split('-') + [self.build_id('ctask', ctid)]
            new_msg = ':'.join(['queue', self.build_id('cq', ctid), '-'.join(new_params)])
            self.qtask_queues[dst_ix].put(new_msg)
            count -= 1
            if count < 1:
                break
            gevent.sleep(0)
        if self.debug:
            print('ctask({0}): exit'.format(ctid))

def run_qmark(num_qtasks, num_ctasks, num_runs = 1):
    ''' Simple CPU benchmark test

    Qtasks are greenlet micro servers. They read messages from their queues
    then post them onto queues referred to in the messages.

    Ctasks are greenlet micro clients. They create a single message, then pass
    it through all existing qtasks sequentially.  They exit when the message
    passes through all qtasks.  The test ends when all ctasks complete.

    num_qtasks      -- number of queue micro servers
    num_ctasks      -- number of client tasks
    num_runs        -- number of qmark runs
    '''

    if num_runs < 1:
        num_runs = 1
    runs = []   # List of bench mark results
    for ix in range(num_runs):
        qmark = QMark(num_qtasks, num_ctasks)
        result = qmark.run()
        runs.append(result)
    return runs

def qmark():
    ''' Return cpu performance indicator
    '''
    results = run_qmark(QTASKS, CTASKS, RUNS)
    avg = sum(results)/len(results)
    return int(1000.0/avg)


if __name__ == '__main__':
    import math
    num_qtasks = QTASKS
    num_ctasks = CTASKS
    results = run_qmark(num_qtasks, num_ctasks, RUNS)
    num_runs = len(results)
    avg = sum(results)/num_runs
    sqr = [(x-avg)*(x-avg) for x in results]
    stdev = math.sqrt(sum(sqr)/num_runs)
    qm = int(1000.0/avg)
    print('Simple CPU benchmark test')
    print('   number of qtasks: {0}'.format(num_qtasks))
    print('   number of ctasks: {0}'.format(num_ctasks))
    print('   results [s]:      {0}'.format(' '.join(['{0:07.3f}'.format(x) for x in results])))
    print('   average [s]:      {0:07.3f}'.format(avg))
    print('   stdev   [s]:      {0:07.3f}'.format(stdev))
    print('   qmark:            {0}'.format(qm))
