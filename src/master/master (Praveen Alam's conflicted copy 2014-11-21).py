import da
PatternExpr_0 = da.pat.TuplePattern([da.pat.ConstantPattern('REGISTER'), da.pat.FreePattern('bankName')])
PatternExpr_1 = da.pat.FreePattern('client')
PatternExpr_2 = da.pat.TuplePattern([da.pat.ConstantPattern('HEAD_INFO'), da.pat.FreePattern('bankName')])
PatternExpr_3 = da.pat.FreePattern('client')
PatternExpr_4 = da.pat.TuplePattern([da.pat.ConstantPattern('TAIL_INFO'), da.pat.FreePattern('bankName')])
PatternExpr_5 = da.pat.FreePattern('client')
PatternExpr_6 = da.pat.TuplePattern([da.pat.ConstantPattern('JOIN_AS_TAIL'), da.pat.FreePattern('bankName')])
PatternExpr_7 = da.pat.FreePattern('p')
PatternExpr_8 = da.pat.TuplePattern([da.pat.ConstantPattern('ACK_AS_TAIL')])
PatternExpr_9 = da.pat.FreePattern('server')
PatternExpr_10 = da.pat.TuplePattern([da.pat.ConstantPattern('PREDECESSOR_RECONCILE'), da.pat.FreePattern('lastProcessedReqId')])
PatternExpr_11 = da.pat.FreePattern('srvr')
PatternExpr_12 = da.pat.TuplePattern([da.pat.ConstantPattern('HEARTBEAT')])
PatternExpr_13 = da.pat.FreePattern('srvr')
import sys
import time
import json
from common_headers import serverRole


class Bank:

    def __init__(self, name, head, tail):
        self.name = name
        self.servers = []
        self.clients = []
        self.head = head
        self.tail = tail


class Server:

    def __init__(self, p):
        self.p = p
        self.successor = None
        self.predecessor = None
        self.role = serverRole.undefined
        self.lastHeard = time.time()


class Master(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._events.extend([
        da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_0', PatternExpr_0, sources=[PatternExpr_1], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_0]), 
        da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_1', PatternExpr_2, sources=[PatternExpr_3], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_1]), 
        da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_2', PatternExpr_4, sources=[PatternExpr_5], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_2]), 
        da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_3', PatternExpr_6, sources=[PatternExpr_7], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_3]), 
        da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_4', PatternExpr_8, sources=[PatternExpr_9], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_4]), 
        da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_5', PatternExpr_10, sources=[PatternExpr_11], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_5]), 
        da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_6', PatternExpr_12, sources=[PatternExpr_13], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_6])])

    def main(self):
        self.output(' Master: Master running')
        _st_label_145 = 0
        while (_st_label_145 == 0):
            _st_label_145+=1
            if 0:
                _st_label_145+=1
            else:
                super()._label('_st_label_145', block=True)
                _st_label_145-=1

    def setup(self, bankList):
        self.bankList = bankList
        pass

    def _Master_handler_0(self, bankName, client):
        self.output('Master: registering client')
        bankIndex = [x for x in self.bankList if (x.name == bankName)]
        if (len(bankIndex) == 0):
            return 
        bankIndex[0].clients.append(client)
        self._send(('HEAD_UPDATE', bankName, bankIndex[0].head), client)
        self._send(('TAIL_UPDATE', bankName, bankIndex[0].tail), client)
    _Master_handler_0._labels = None
    _Master_handler_0._notlabels = None

    def _Master_handler_1(self, bankName, client):
        bankIndex = [x for x in self.bankList if (x.name == bankName)]
        if (len(bankIndex) == 0):
            return 
        bankIndex[0].clients.append(client)
        self._send(('HEAD_UPDATE', bankName, bankIndex[0].head), client)
    _Master_handler_1._labels = None
    _Master_handler_1._notlabels = None

    def _Master_handler_2(self, client, bankName):
        bankIndex = [x for x in self.bankList if (x.name == bankName)]
        if (len(bankIndex) == 0):
            return 
        bankIndex[0].clients.append(client)
        self._send(('TAIL_UPDATE', bankName, bankIndex[0].tail), client)
    _Master_handler_2._labels = None
    _Master_handler_2._notlabels = None

    def _Master_handler_3(self, p, bankName):
        for bank in self.bankList:
            if (bank.name == bankName):
                bankIndex = bank
        'if(bankIndex==None):\n\t\t\ts = Server(p)\n\t\t\tb = Bank(bankName,p,p)\n\t\t\tb.servers.append(s)\n\t\t\toutput("Master: first tail joined")\n\t\t\tsend((\'ACK_JOIN_AS_TAIL\',bankIndex.tail), to=p)'
        if (len(bankIndex.servers) == 0):
            s = Server(p)
            bankIndex.servers.append(s)
            bankIndex.head = p
            bankIndex.tail = p
            s.role = serverRole.stand_alone
            self.output(('Master:first tail joined ' + bankName))
            self._send(('STAND_ALONE',), p)
        else:
            s = Server(p)
            bankIndex.servers.append(s)
            temp = bankIndex.tail
            s.role = serverRole.waiting_tobe_tail
            tailInList = [x for x in bankIndex.servers if (x.p == bankIndex.tail)]
            tailInList[0].successor = s
            s.predecessor = tailInList[0]
            self._send(('ACK_JOIN_AS_TAIL', temp), p)
            self.output(('Master: new tail joinining ' + str(bankName)))
            self._send(('NEW_TAIL_JOINED', p), bankIndex.tail)
    _Master_handler_3._labels = None
    _Master_handler_3._notlabels = None

    def _Master_handler_4(self, server):
        self.output('Master: ack as tail received')
        for i in range(0, len(self.bankList)):
            for j in self.bankList[i].servers:
                if (j.p == server):
                    tailInList = [x for x in self.bankList[i].servers if (x.p == self.bankList[i].tail)]
                    self.bankList[i].tail = server
                    j.role = serverRole.tail
                    if (tailInList[0].role == serverRole.stand_alone):
                        tailInList[0].role = serverRole.head
                        self._send(('PROMOTE_TO_HEAD',), tailInList[0].p)
                    else:
                        tailInList[0].role = serverRole.internal
                        self._send(('BECOME_INTERNAL',), tailInList[0].p)
                    self.output(((('Master: BankName :' + self.bankList[i].name) + ' New Tail ') + str(server)))
    _Master_handler_4._labels = None
    _Master_handler_4._notlabels = None

    def _Master_handler_5(self, lastProcessedReqId, srvr):
        for bank in self.bankList:
            for server in bank.servers:
                if (server.p == srvr):
                    server.predecessor.successor = server
                    self.output(((('Master ' + str(server.predecessor.p)) + ' has new successor ') + str(server.p)))
                    self.output(('Master: last request id processed ' + str(lastProcessedReqId)))
                    self._send(('SUCCESSOR_UPDATE', server.p, lastProcessedReqId), server.predecessor.p)
                    break
    _Master_handler_5._labels = None
    _Master_handler_5._notlabels = None

    def _Master_handler_6(self, srvr):
        for bank in self.bankList:
            for server in bank.servers:
                if (server.p == srvr):
                    server.lastHeard = time.time()
                    break
        for bank in self.bankList:
            for server in bank.servers:
                if ((time.time() - server.lastHeard) >= 10):
                    self.output((('Master: ' + str(server.p)) + 'died'))
                    if (server.role == serverRole.head):
                        bank.head = server.successor.p
                        if (server.successor.role == serverRole.tail):
                            server.successor.role = serverRole.stand_alone
                        else:
                            self.output((('Master: ' + str(bank.head)) + 'will be new head'))
                            server.successor.role = serverRole.head
                        self._send(('PROMOTE_TO_HEAD',), bank.head)
                        for client in bank.clients:
                            self._send(('HEAD_UPDATE', bank.name, bank.head), client)
                        bank.servers.remove(server)
                    elif (server.role == serverRole.tail):
                        if (not (server.successor == None)):
                            bank.tail = server.successor.p
                            self.output('tail died during chain extension')
                            server.successor.role = serverRole.tail
                            server.successor.predecessor = server.predecessor
                            self._send(('PROMOTE_TO_TAIL',), server.successor.p)
                            self._send(('PREDECESSOR_UPDATE', server.predecessor.p), server.successor.p)
                            bank.servers.remove(server)
                            continue
                        bank.tail = server.predecessor.p
                        if (server.predecessor.role == serverRole.head):
                            server.predecessor.role = serverRole.stand_alone
                        else:
                            self.output((('Master: ' + str(server.predecessor.p)) + 'will be new tail'))
                            server.predecessor.role = serverRole.tail
                        self._send(('PROMOTE_TO_TAIL',), bank.tail)
                        for client in bank.clients:
                            self._send(('TAIL_UPDATE', bank.name, bank.tail), client)
                        bank.servers.remove(server)
                    elif (server.role == serverRole.internal):
                        self.output(((('Master: ' + str(server.successor.p)) + ' has new predecessor ') + str(server.predecessor.p)))
                        server.successor.predecessor = server.predecessor
                        self._send(('PREDECESSOR_UPDATE', server.predecessor.p), server.successor.p)
                        bank.servers.remove(server)
                    elif (server.role == serverRole.waiting_tobe_tail):
                        self.output('Master: server which was waiting to be tail died! chain extension aborted')
                        server.predecessor.successor = None
                        bank.servers.remove(server)
                    elif (server.role == serverRole.stand_alone):
                        if (not (server.successor == None)):
                            server.successor.role = serverRole.stand_alone
                            bank.head = server.successor.p
                            bank.tail = server.successor.p
                        bank.servers.remove(server)
    _Master_handler_6._labels = None
    _Master_handler_6._notlabels = None

def main(fileName):
    json_data = open(fileName)
    info = json.load(json_data)
    bankList = []
    for bank in info['master']['banks']:
        temp = Bank(bank['bank_name'], None, None)
        bankList.append(temp)
    master = da.api.new(Master)
    da.api.setup(master, [bankList])
    da.api.start(master)
    return master
