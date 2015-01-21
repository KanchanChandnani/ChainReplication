
import da
PatternExpr_0 = da.pat.TuplePattern([da.pat.ConstantPattern('BALANCE'), da.pat.FreePattern('reqID'), da.pat.FreePattern('accountNumber'), da.pat.ConstantPattern(0)])
PatternExpr_1 = da.pat.FreePattern('p')
PatternExpr_2 = da.pat.TuplePattern([da.pat.ConstantPattern('DEPOSIT'), da.pat.FreePattern('reqID'), da.pat.FreePattern('accountNumber'), da.pat.FreePattern('amount')])
PatternExpr_3 = da.pat.FreePattern('p')
PatternExpr_4 = da.pat.TuplePattern([da.pat.ConstantPattern('WITHDRAW'), da.pat.FreePattern('reqID'), da.pat.FreePattern('accountNumber'), da.pat.FreePattern('amount')])
PatternExpr_5 = da.pat.FreePattern('p')
PatternExpr_6 = da.pat.TuplePattern([da.pat.ConstantPattern('TRANSFER'), da.pat.FreePattern('reqID'), da.pat.FreePattern('accountNumber'), da.pat.FreePattern('amount'), da.pat.FreePattern('destBank'), da.pat.FreePattern('destAccount')])
PatternExpr_7 = da.pat.FreePattern('p')
PatternExpr_8 = da.pat.TuplePattern([da.pat.ConstantPattern('HEAD_UPDATE'), da.pat.FreePattern('bank'), da.pat.FreePattern('head')])
PatternExpr_9 = da.pat.FreePattern('master')
PatternExpr_11 = da.pat.TuplePattern([da.pat.ConstantPattern('REPLY'), da.pat.BoundPattern('_BoundPattern35_'), da.pat.FreePattern('resultDeposit'), da.pat.FreePattern(None), da.pat.FreePattern(None)])
PatternExpr_13 = da.pat.TuplePattern([da.pat.ConstantPattern('ACK_SYNC'), da.pat.FreePattern('reqID')])
PatternExpr_14 = da.pat.FreePattern('p')
PatternExpr_15 = da.pat.TuplePattern([da.pat.ConstantPattern('ACCOUNT_UPDATE'), da.pat.FreePattern('reqID'), da.pat.FreePattern('client'), da.pat.FreePattern('result'), da.pat.FreePattern('accountNumber'), da.pat.FreePattern('amount'), da.pat.FreePattern('balance'), da.pat.FreePattern('destBank'), da.pat.FreePattern('destAccount')])
PatternExpr_16 = da.pat.FreePattern('pred')
PatternExpr_17 = da.pat.TuplePattern([da.pat.ConstantPattern('STAND_ALONE')])
PatternExpr_18 = da.pat.FreePattern('master')
PatternExpr_19 = da.pat.TuplePattern([da.pat.ConstantPattern('PROMOTE_TO_HEAD')])
PatternExpr_20 = da.pat.FreePattern('master')
PatternExpr_21 = da.pat.TuplePattern([da.pat.ConstantPattern('PROMOTE_TO_TAIL')])
PatternExpr_22 = da.pat.FreePattern('master')
PatternExpr_23 = da.pat.TuplePattern([da.pat.ConstantPattern('PREDECESSOR_UPDATE'), da.pat.FreePattern('newPred')])
PatternExpr_24 = da.pat.FreePattern('master')
PatternExpr_25 = da.pat.TuplePattern([da.pat.ConstantPattern('NEW_TAIL_JOINED'), da.pat.FreePattern('tail')])
PatternExpr_26 = da.pat.FreePattern('master')
PatternExpr_27 = da.pat.TuplePattern([da.pat.ConstantPattern('SYNC_PROCTABLE'), da.pat.FreePattern('ReqId'), da.pat.FreePattern('Client'), da.pat.FreePattern('AccountNumber'), da.pat.FreePattern('Amount'), da.pat.FreePattern('Balance'), da.pat.FreePattern('Result'), da.pat.FreePattern('destBank'), da.pat.FreePattern('destAccount')])
PatternExpr_28 = da.pat.FreePattern('pred')
PatternExpr_29 = da.pat.TuplePattern([da.pat.ConstantPattern('BECOME_INTERNAL')])
PatternExpr_30 = da.pat.FreePattern('master')
PatternExpr_31 = da.pat.TuplePattern([da.pat.ConstantPattern('END_OF_SENT')])
PatternExpr_32 = da.pat.FreePattern('pred')
PatternExpr_33 = da.pat.TuplePattern([da.pat.ConstantPattern('ACK_JOIN_AS_TAIL'), da.pat.FreePattern('pred')])
PatternExpr_34 = da.pat.FreePattern('master')
PatternExpr_35 = da.pat.TuplePattern([da.pat.ConstantPattern('SUCCESSOR_UPDATE'), da.pat.FreePattern('newSuc'), da.pat.FreePattern('sucLatestReqID')])
PatternExpr_36 = da.pat.FreePattern('master')
PatternExpr_37 = da.pat.TuplePattern([da.pat.ConstantPattern('KILL_MYSELF')])
import sys
import json
import time
import threading
from common_headers import serverRole, opOutcome

class Server(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._ServerReceivedEvent_5 = []
        self._ServerReceivedEvent_18 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_0', PatternExpr_0, sources=[PatternExpr_1], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_0]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_1', PatternExpr_2, sources=[PatternExpr_3], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_1]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_2', PatternExpr_4, sources=[PatternExpr_5], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_2]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_3', PatternExpr_6, sources=[PatternExpr_7], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_3]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_4', PatternExpr_8, sources=[PatternExpr_9], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_4]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_5', PatternExpr_11, sources=None, destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_6', PatternExpr_13, sources=[PatternExpr_14], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_5]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_7', PatternExpr_15, sources=[PatternExpr_16], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_6]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_8', PatternExpr_17, sources=[PatternExpr_18], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_7]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_9', PatternExpr_19, sources=[PatternExpr_20], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_8]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_10', PatternExpr_21, sources=[PatternExpr_22], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_9]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_11', PatternExpr_23, sources=[PatternExpr_24], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_10]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_12', PatternExpr_25, sources=[PatternExpr_26], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_11]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_13', PatternExpr_27, sources=[PatternExpr_28], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_12]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_14', PatternExpr_29, sources=[PatternExpr_30], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_13]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_15', PatternExpr_31, sources=[PatternExpr_32], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_14]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_16', PatternExpr_33, sources=[PatternExpr_34], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_15]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_17', PatternExpr_35, sources=[PatternExpr_36], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_16]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_18', PatternExpr_37, sources=None, destinations=None, timestamps=None, record_history=True, handlers=[])])

    def setup(self, bankName, master, delay, ltime, sendLife, recvLife, servId):
        self.master = master
        self.recvLife = recvLife
        self.delay = delay
        self.servId = servId
        self.sendLife = sendLife
        self.ltime = ltime
        self.bankName = bankName
        self.processedTrans = []
        self.sentTrans = []
        self.transfers = []
        self.successor = None
        self.predecessor = None
        self.accounts = {}
        self.role = serverRole.undefined
        self.noOfSends = 0
        self.noOfRecvs = 0
        self.heads = set()

    def _da_run_internal(self):
        self.register()
        threading.Timer(self.ltime, self.killMySelf).start()
        self.sendHeartBeat()

        def ExistentialOpExpr_2():
            for (_, _, (_ConstantPattern119_,)) in self._ServerReceivedEvent_18:
                if (_ConstantPattern119_ == 'KILL_MYSELF'):
                    if True:
                        return True
            return False
        _st_label_243 = 0
        while (_st_label_243 == 0):
            _st_label_243 += 1
            if ExistentialOpExpr_2():
                _st_label_243 += 1
            else:
                super()._label('_st_label_243', block=True)
                _st_label_243 -= 1

    def sendDepositForTransfer(self, reqID, resultWithdraw, client, accountNumber, amount, balance, destBank, destAccount, nretries):
        self._send(('HEAD_INFO', destBank), self.master)
        head = None

        def ExistentialOpExpr_0():
            nonlocal head
            for (_BoundPattern31_, head) in self.heads:
                if (_BoundPattern31_ == destBank):
                    if True:
                        return True
            return False
        _st_label_115 = 0
        while (_st_label_115 == 0):
            _st_label_115 += 1
            if ExistentialOpExpr_0():
                _st_label_115 += 1
            else:
                super()._label('_st_label_115', block=True)
                _st_label_115 -= 1
        self.heads.discard((destBank, head))
        self.output(((((('Server: Sending Deposit(part of transfer) ' + reqID) + ' ') + str(amount)) + ' to ') + str(head)))
        self._send(('DEPOSIT', reqID, destAccount, amount), head)
        resultDeposit = None

        def ExistentialOpExpr_1():
            nonlocal resultDeposit
            for (_, _, (_ConstantPattern47_, _BoundPattern48_, resultDeposit, _, _)) in self._ServerReceivedEvent_5:
                if (_ConstantPattern47_ == 'REPLY'):
                    if (_BoundPattern48_ == reqID):
                        if True:
                            return True
            return False
        _st_label_119 = 0
        self._timer_start()
        while (_st_label_119 == 0):
            _st_label_119 += 1
            if ExistentialOpExpr_1():
                self.output(('Server: Transfer Done  ' + reqID))
                self._send(('REPLY', reqID, resultWithdraw, accountNumber, balance), client)
                self.transfers.remove(reqID)
                self._send(('ACK_SYNC', reqID), self.predecessor)
                return
                _st_label_119 += 1
            elif self._timer_expired:
                if (nretries > 0):
                    self.output(('Server: Resending deposit(transfer) for request Id ' + reqID))
                    self.sendDepositForTransfer(reqID, resultWithdraw, client, accountNumber, amount, balance, destBank, destAccount, (nretries - 1))
                else:
                    self.output(('Aborting transfer ' + str(reqID)))
                _st_label_119 += 1
            else:
                super()._label('_st_label_119', block=True, timeout=10)
                _st_label_119 -= 1

    def sync(self, reqID, result, client, accountNumber, amount, balance, destBank, destAccount):
        if (self.role == serverRole.tail):
            self.noOfSends += 1
            if (self.noOfSends >= self.sendLife):
                self.output((str(self.id) + ' is going to die'))
                self._send(('KILL_MYSELF',), self.id)
            else:
                if self.successor:
                    self._send(('ACCOUNT_UPDATE', reqID, client, result, accountNumber, amount, balance, destBank, destAccount), self.successor)
                if ((not (destBank == None)) and (result == opOutcome.processed)):
                    self.transfers.append(reqID)
                    nretries = 3
                    self.sendDepositForTransfer(reqID, result, client, accountNumber, amount, balance, destBank, destAccount, nretries)
                else:
                    self._send(('REPLY', reqID, result, accountNumber, balance), client)
                    self._send(('ACK_SYNC', reqID), self.predecessor)
        elif (self.role == serverRole.waiting_tobe_tail):
            pass
        else:
            if (not destBank):
                self.sentTrans.append(reqID)
            else:
                self.transfers.append(reqID)
            if self.successor:
                self._send(('ACCOUNT_UPDATE', reqID, client, result, accountNumber, amount, balance, destBank, destAccount), self.successor)

    def sendHeartBeat(self):
        self._send(('HEARTBEAT',), self.master)
        threading.Timer(5, self.sendHeartBeat).start()

    def register(self):
        time.sleep(self.delay)
        self._send(('JOIN_AS_TAIL', self.bankName), self.master)

    def killMySelf(self):
        self.output(('Server: killing process' + str(self.id)))
        self._send(('KILL_MYSELF',), self.id)

    def reqMatch(self, reqTup, List):
        for tup in List:
            if ((tup[0] == reqTup[0]) and (tup[4] == reqTup[2])):
                return 'CONSISTENT'
            elif (tup[0] == reqTup[0]):
                return 'INCONSISTENT'
        return 'NEW_REQ'

    def getProcResult(self, reqID, List):
        for tup in List:
            if (tup[0] == reqID):
                return tup[5]
        return None

    def _Server_handler_0(self, accountNumber, reqID, p):
        self.noOfRecvs += 1
        self.noOfSends += 1
        if (self.noOfRecvs >= self.recvLife):
            self._send(('KILL_MYSELF',), self.id)
        elif ((accountNumber in self.accounts) and (self.role == serverRole.tail)):
            self._send(('REPLY', reqID, opOutcome.processed, accountNumber, self.accounts[accountNumber]), p)
        else:
            self._send(('REPLY', reqID, opOutcome.illegal, accountNumber, 0), p)
    _Server_handler_0._labels = None
    _Server_handler_0._notlabels = None

    def _Server_handler_1(self, reqID, p, amount, accountNumber):
        self.noOfRecvs += 1
        if (self.noOfRecvs >= self.recvLife):
            self._send(('KILL_MYSELF',), self.id)
        elif (self.reqMatch((reqID, accountNumber, amount), self.processedTrans) == 'CONSISTENT'):
            self.sync(reqID, opOutcome.processed, p, accountNumber, amount, self.accounts[accountNumber], None, None)
        elif (self.reqMatch((reqID, accountNumber, amount), self.processedTrans) == 'INCONSTENT'):
            self.sync(reqID, opOutcome.incon_hist, p, accountNumber, amount, self.accounts[accountNumber], None, None)
        elif (amount < 0):
            if (accountNumber in self.accounts):
                self.sync(reqID, opOutcome.invalid_amount, p, accountNumber, amount, self.accounts[accountNumber], None, None)
            else:
                self.sync(reqID, opOutcome.illegal, p, accountNumber, amount, 0, None, None)
        elif (accountNumber in self.accounts):
            if ((self.role == serverRole.head) or (self.role == serverRole.stand_alone)):
                self.accounts[accountNumber] += amount
                self.processedTrans.append((reqID, p, accountNumber, amount, self.accounts[accountNumber], opOutcome.processed, None, None))
                self.sync(reqID, opOutcome.processed, p, accountNumber, amount, self.accounts[accountNumber], None, None)
            else:
                self.output('server: Non-head received deposit request')
                self.sync(reqID, opOutcome.illegal, p, accountNumber, amount, 0, None, None)
        elif ((self.role == serverRole.head) or (self.role == serverRole.stand_alone)):
            self.output(('Server: Account does not exist ' + str(reqID)))
            self.accounts[accountNumber] = amount
            self.processedTrans.append((reqID, p, accountNumber, amount, self.accounts[accountNumber], opOutcome.processed, None, None))
            self.sync(reqID, opOutcome.processed, p, accountNumber, amount, self.accounts[accountNumber], None, None)
        else:
            self.output('Server: account Does Not exist,Non-Head ')
            self.sync(reqID, opOutcome.illegal, p, accountNumber, amount, 0, None, None)
    _Server_handler_1._labels = None
    _Server_handler_1._notlabels = None

    def _Server_handler_2(self, p, accountNumber, reqID, amount):
        self.noOfRecvs += 1
        if (self.noOfRecvs >= self.recvLife):
            self._send(('KILL_MYSELF',), self.id)
        elif (self.reqMatch((reqID, accountNumber, amount), self.processedTrans) == 'CONSISTENT'):
            self.sync(reqID, opOutcome.processed, p, accountNumber, amount, self.accounts[accountNumber], None, None)
        elif (self.reqMatch((reqID, accountNumber, amount), self.processedTrans) == 'INCONSTENT'):
            self.sync(reqID, opOutcome.incon_hist, p, accountNumber, amount, self.accounts[accountNumber], None, None)
        elif (amount < 0):
            if (accountNumber in self.accounts):
                self.sync(reqID, opOutcome.invalid_amount, p, accountNumber, amount, self.accounts[accountNumber], None, None)
            else:
                self.sync(reqID, opOutcome.illegal, p, accountNumber, amount, 0, None, None)
        elif (accountNumber in self.accounts):
            if ((self.role == serverRole.head) or (self.role == serverRole.stand_alone)):
                if (amount <= self.accounts[accountNumber]):
                    self.accounts[accountNumber] -= amount
                    self.processedTrans.append((reqID, p, accountNumber, amount, self.accounts[accountNumber], opOutcome.processed, None, None))
                    self.sync(reqID, opOutcome.processed, p, accountNumber, amount, self.accounts[accountNumber], None, None)
                else:
                    self.sync(reqID, opOutcome.insuf_funds, p, accountNumber, amount, self.accounts[accountNumber], None, None)
            else:
                self.output(('Server: Non-head received request ' + self.bankName))
                self.sync(reqID, opOutcome.illegal, p, accountNumber, amount, 0, None, None)
        elif ((self.role == serverRole.head) or (self.role == serverRole.stand_alone)):
            self.accounts[accountNumber] = 0
            self.processedTrans.append((reqID, p, accountNumber, amount, self.accounts[accountNumber], opOutcome.processed, None, None))
            self.sync(reqID, opOutcome.insuf_funds, p, accountNumber, amount, self.accounts[accountNumber], None, None)
        else:
            self.output('Server: account Does Not exist,Non-Head')
            self.sync(reqID, opOutcome.illegal, p, accountNumber, amount, 0, None, None)
    _Server_handler_2._labels = None
    _Server_handler_2._notlabels = None

    def _Server_handler_3(self, destAccount, reqID, accountNumber, p, destBank, amount):
        self.noOfRecvs += 1
        if (self.noOfRecvs >= self.recvLife):
            self._send(('KILL_MYSELF',), self.id)
        else:
            if (self.reqMatch((reqID, accountNumber, amount), self.processedTrans) == 'CONSISTENT'):
                self.output('   ')
                self.output('   ')
                self.output('   ')
                self.output(('Client retransmitted transfer request: ' + reqID))
                self.output('   ')
                self.output('   ')
                self.output('   ')
                result = self.getProcResult(reqID, self.processedTrans)
                self.sync(reqID, result, p, accountNumber, amount, self.accounts[accountNumber], None, None)
            elif (self.reqMatch((reqID, accountNumber, amount), self.processedTrans) == 'INCONSTENT'):
                self.sync(reqID, opOutcome.incon_hist, p, accountNumber, amount, self.accounts[accountNumber], destBank, destAccount)
            if (accountNumber in self.accounts):
                if ((self.role == serverRole.head) or (self.role == serverRole.stand_alone)):
                    if (amount <= self.accounts[accountNumber]):
                        self.accounts[accountNumber] -= amount
                        self.processedTrans.append((reqID, p, accountNumber, amount, self.accounts[accountNumber], opOutcome.processed, destBank, destAccount))
                        self.sync(reqID, opOutcome.processed, p, accountNumber, amount, self.accounts[accountNumber], destBank, destAccount)
                    else:
                        self.sync(reqID, opOutcome.insuf_funds, p, accountNumber, amount, self.accounts[accountNumber], destBank, destAccount)
                else:
                    self.output(('Server: Non-head received request ' + self.bankName))
                    self.sync(reqID, opOutcome.illegal, p, accountNumber, amount, 0, destBank, destAccount)
            elif ((self.role == serverRole.head) or (self.role == serverRole.stand_alone)):
                self.accounts[accountNumber] = 0
                self.processedTrans.append((reqID, p, accountNumber, amount, self.accounts[accountNumber], opOutcome.processed, None, None))
                self.sync(reqID, opOutcome.insuf_funds, p, accountNumber, amount, self.accounts[accountNumber], destAccount, destAccount)
            else:
                self.output('Server: account Does Not exist,Non-Head')
                self.sync(reqID, opOutcome.illegal, p, accountNumber, amount, 0, destAccount, destAccount)
    _Server_handler_3._labels = None
    _Server_handler_3._notlabels = None

    def _Server_handler_4(self, bank, master, head):
        self.heads.add((bank, head))
    _Server_handler_4._labels = None
    _Server_handler_4._notlabels = None

    def _Server_handler_5(self, reqID, p):
        if (reqID in self.sentTrans):
            self.sentTrans.remove(reqID)
            if self.predecessor:
                self._send(('ACK_SYNC', reqID), self.predecessor)
        if (reqID in self.transfers):
            self.transfers.remove(reqID)
            if self.predecessor:
                self._send(('ACK_SYNC', reqID), self.predecessor)
    _Server_handler_5._labels = None
    _Server_handler_5._notlabels = None

    def _Server_handler_6(self, amount, pred, reqID, balance, client, destBank, result, destAccount, accountNumber):
        if ((not ((reqID, client, accountNumber, amount, balance, result, destBank, destAccount) in self.processedTrans)) and ((result == opOutcome.processed) or (result == opOutcome.insuf_funds))):
            self.accounts[accountNumber] = balance
            self.processedTrans.append((reqID, client, accountNumber, amount, self.accounts[accountNumber], result, destBank, destAccount))
        if (not (reqID in self.sentTrans)):
            self.sync(reqID, result, client, accountNumber, amount, balance, destBank, destAccount)
    _Server_handler_6._labels = None
    _Server_handler_6._notlabels = None

    def _Server_handler_7(self, master):
        self.output(('Server: I am stand alone ' + self.bankName))
        self.role = serverRole.stand_alone
    _Server_handler_7._labels = None
    _Server_handler_7._notlabels = None

    def _Server_handler_8(self, master):
        self.output('Server: Received promote to head')
        self.predecessor = None
        if (self.role == serverRole.tail):
            self.role = serverRole.stand_alone
        else:
            self.role = serverRole.head
    _Server_handler_8._labels = None
    _Server_handler_8._notlabels = None

    def _Server_handler_9(self, master):
        self.successor = None
        if (self.role == serverRole.head):
            self.role = serverRole.stand_alone
        else:
            self.output('Server: promoted to tail')
            self.role = serverRole.tail
        if (not (len(self.transfers) == 0)):
            for reqID in self.transfers:
                tup = [x for x in self.processedTrans if (x[0] == reqID)]
                nretries = 3
                self.output('Resending deposit(as a part of transfer) as the new tail')
                self.sendDepositForTransfer(tup[0][0], tup[0][5], tup[0][1], tup[0][2], tup[0][3], tup[0][4], tup[0][6], tup[0][7], nretries)
    _Server_handler_9._labels = None
    _Server_handler_9._notlabels = None

    def _Server_handler_10(self, newPred, master):
        self.predecessor = newPred
        self.output(('Server: I have a new predecessor ' + str(newPred)))
        if len(self.sentTrans):
            lastReqID = self.sentTrans[(- 1)]
        else:
            lastReqID = 0
        self._send(('PREDECESSOR_RECONCILE', lastReqID), master)
    _Server_handler_10._labels = None
    _Server_handler_10._notlabels = None

    def _Server_handler_11(self, tail, master):
        self.output(('Server: a new tail joined ' + self.bankName))
        if (self.role == serverRole.stand_alone):
            self.role = serverRole.head
        'elif( role == serverRole.tail):\n\t\t\trole = serverRole.transient_tail'
        self.successor = tail
        for tup in self.processedTrans:
            self.output(('Server: Sending to new tail ReqId: ' + tup[0]))
            self._send(('SYNC_PROCTABLE', tup[0], tup[1], tup[2], tup[3], tup[4], tup[5], tup[6], tup[7]), self.successor)
        self._send(('END_OF_SENT',), self.successor)
    _Server_handler_11._labels = None
    _Server_handler_11._notlabels = None

    def _Server_handler_12(self, Client, destBank, AccountNumber, destAccount, Amount, Balance, pred, ReqId, Result):
        self.output('Server: Syncing with predecessor')
        self.processedTrans.append((ReqId, Client, AccountNumber, Amount, Balance, Result, destBank, destAccount))
    _Server_handler_12._labels = None
    _Server_handler_12._notlabels = None

    def _Server_handler_13(self, master):
        self.role = serverRole.internal
    _Server_handler_13._labels = None
    _Server_handler_13._notlabels = None

    def _Server_handler_14(self, pred):
        self.role = serverRole.tail
        self._send(('ACK_AS_TAIL',), self.master)
    _Server_handler_14._labels = None
    _Server_handler_14._notlabels = None

    def _Server_handler_15(self, master, pred):
        self.predecessor = pred
        self.role = serverRole.waiting_tobe_tail
    _Server_handler_15._labels = None
    _Server_handler_15._notlabels = None

    def _Server_handler_16(self, sucLatestReqID, newSuc, master):
        self.output(('Server: I have a new successor ' + str(newSuc)))
        self.successor = newSuc
        if (sucLatestReqID in self.sentTrans):
            indx = self.sentTrans.index(sucLatestReqID)
            for reqID in self.sentTrans[indx:]:
                for tup in self.processedTrans:
                    if (reqID == tup[0]):
                        self.output(('Sending to new successor ReqId: ' + tup[0]))
                        self._send(('ACCOUNT_UPDATE', tup[0], tup[1], opOutcome.processed, tup[2], tup[3], tup[4], tup[6], tup[7]), self.successor)
        elif (sucLatestReqID == 0):
            for reqID in self.sentTrans:
                for tup in self.processedTrans:
                    if (reqID == tup[0]):
                        self.output(('Server: Sending to new successor ReqId: ' + tup[0]))
                        self._send(('ACCOUNT_UPDATE', tup[0], tup[1], opOutcome.processed, tup[2], tup[3], tup[4], tup[6], tup[7]), self.successor)
    _Server_handler_16._labels = None
    _Server_handler_16._notlabels = None

def main(master, fileName):
    json_data = open(fileName)
    info = json.load(json_data)
    da.api.config(channel='reliable')
    bank_srv_list = set()
    bank_srv_dict = {}
    for bank in info['master']['banks']:
        delayList = []
        lifeTimeList = []
        sendLifeList = []
        recvLifeList = []
        servIdList = []
        bank_srv_list = da.api.new(Server, num=bank['bank_clength'])
        for server in bank['servers']:
            delayList.append(server['serv_sdelay'])
            lifeTimeList.append(server['serv_ltime'])
            servIdList.append(server['serv_id'])
            sendLifeList.append(server['serv_snd_life'])
            recvLifeList.append(server['serv_rcv_life'])
        i = 0
        for serv in bank_srv_list:
            da.api.setup({serv}, [bank['bank_name'], master, delayList[i], lifeTimeList[i], sendLifeList[i], recvLifeList[i], servIdList[i]])
            i = (i + 1)
        bank_srv_dict[bank['bank_name']] = bank_srv_list
    for bank in bank_srv_dict:
        da.api.start(bank_srv_dict[bank])
