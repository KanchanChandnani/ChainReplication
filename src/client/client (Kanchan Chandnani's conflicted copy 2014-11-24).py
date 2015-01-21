
import da
PatternExpr_0 = da.pat.TuplePattern([da.pat.ConstantPattern('REPLY'), da.pat.FreePattern('reqID'), da.pat.FreePattern('outcome'), da.pat.FreePattern('accountNumber'), da.pat.FreePattern('balance')])
PatternExpr_1 = da.pat.TuplePattern([da.pat.ConstantPattern('HEAD_UPDATE'), da.pat.FreePattern('bankName'), da.pat.FreePattern('head')])
PatternExpr_2 = da.pat.FreePattern('master')
PatternExpr_3 = da.pat.TuplePattern([da.pat.ConstantPattern('TAIL_UPDATE'), da.pat.FreePattern('bankName'), da.pat.FreePattern('tail')])
PatternExpr_4 = da.pat.TuplePattern([da.pat.ConstantPattern('REPLY'), da.pat.BoundPattern('_BoundPattern16_'), da.pat.FreePattern(None), da.pat.FreePattern(None), da.pat.FreePattern(None)])
PatternExpr_6 = da.pat.TuplePattern([da.pat.ConstantPattern('REPLY'), da.pat.BoundPattern('_BoundPattern34_'), da.pat.FreePattern(None), da.pat.FreePattern(None), da.pat.FreePattern(None)])
PatternExpr_8 = da.pat.TuplePattern([da.pat.ConstantPattern('REPLY'), da.pat.BoundPattern('_BoundPattern52_'), da.pat.FreePattern(None), da.pat.FreePattern(None), da.pat.FreePattern(None)])
PatternExpr_10 = da.pat.TuplePattern([da.pat.ConstantPattern('REPLY'), da.pat.BoundPattern('_BoundPattern70_'), da.pat.FreePattern('result'), da.pat.FreePattern(None), da.pat.FreePattern(None)])
PatternExpr_12 = da.pat.TuplePattern([da.pat.ConstantPattern('TAIL_UPDATE'), da.pat.FreePattern(None), da.pat.FreePattern(None)])
import json
import sys
from itertools import chain
import time
import random
from common_headers import opOutcome

class Bank():

    def __init__(self, name, head, tail):
        self.name = name
        self.head = head
        self.tail = tail

class Client(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._ClientReceivedEvent_3 = []
        self._ClientReceivedEvent_4 = []
        self._ClientReceivedEvent_5 = []
        self._ClientReceivedEvent_6 = []
        self._ClientReceivedEvent_7 = []
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_0', PatternExpr_0, sources=None, destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_0]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_1', PatternExpr_1, sources=[PatternExpr_2], destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_1]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_2', PatternExpr_3, sources=None, destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_2]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_3', PatternExpr_4, sources=None, destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_4', PatternExpr_6, sources=None, destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_5', PatternExpr_8, sources=None, destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_6', PatternExpr_10, sources=None, destinations=None, timestamps=None, record_history=True, handlers=[]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_7', PatternExpr_12, sources=None, destinations=None, timestamps=None, record_history=True, handlers=[])])

    def setup(self, cId, bank_name, master, bankList, totalReq, getP, depP, withP, reqList, dropRate):
        self.cId = cId
        self.bank_name = bank_name
        self.depP = depP
        self.master = master
        self.totalReq = totalReq
        self.reqList = reqList
        self.getP = getP
        self.withP = withP
        self.bankList = bankList
        self.dropRate = dropRate
        self.seq = (- 1)
        self.T = 6
        self.tryC = 3
        self.responses = []

    def _da_run_internal(self):
        bankName = self.bank_name
        tail = None
        time.sleep(8)
        self.register(self.bank_name)

        def ExistentialOpExpr_4():
            for (_, _, (_ConstantPattern98_, _, _)) in self._ClientReceivedEvent_7:
                if (_ConstantPattern98_ == 'TAIL_UPDATE'):
                    if True:
                        return True
            return False
        _st_label_115 = 0
        while (_st_label_115 == 0):
            _st_label_115 += 1
            if ExistentialOpExpr_4():
                _st_label_115 += 1
            else:
                super()._label('_st_label_115', block=True)
                _st_label_115 -= 1
        reqSent = 0
        acNumber = random.randint(2000, 3000)
        if (len(self.reqList) == 0):
            while (reqSent <= self.totalReq):
                val = random.random()
                resend = random.uniform()
                if (resend < 0.8):
                    resend = 0
                else:
                    resend = 1
                amnt = random.randint(1, 1000)
                if (val <= self.getP):
                    self.getBalance(resend, self.bank_name, acNumber, self.tryC)
                elif (val <= (self.getP + self.depP)):
                    self.deposit(resend, self.bank_name, acNumber, amnt, self.tryC)
                elif (val <= ((self.getP + self.depP) + self.withP)):
                    self.withdraw(resend, self.bank_name, acNumber, amnt, self.tryC)
                reqSent += 1
        else:
            for tup in self.reqList:
                acNumber = tup['accNo']
                if (tup['type'] == 'getBalance'):
                    self.getBalance(tup['resend'], self.bank_name, acNumber, self.tryC)
                elif (tup['type'] == 'deposit'):
                    self.deposit(tup['resend'], self.bank_name, acNumber, tup['amt'], self.tryC)
                elif (tup['type'] == 'withdraw'):
                    self.withdraw(tup['resend'], self.bank_name, acNumber, tup['amt'], self.tryC)
                elif (tup['type'] == 'transfer'):
                    self.transfer(tup['resend'], self.bank_name, acNumber, tup['amt'], tup['destBank'], tup['destAcc'], self.tryC)
        _st_label_143 = 0
        while (_st_label_143 == 0):
            _st_label_143 += 1
            if 0:
                _st_label_143 += 1
            else:
                super()._label('_st_label_143', block=True)
                _st_label_143 -= 1

    def register(self, bankName):
        self._send(('REGISTER', bankName), self.master)

    def retrieveHead(self, bankName):
        self._send(('HEAD_INFO', bankName), self.master)

    def retrieveTail(self, bankName):
        self._send(('TAIL_INFO', bankName), self.master)

    def getBalance(self, resend, bankName, accountNum, nretries):
        if (nretries == self.tryC):
            self.seq = (self.seq + 1)
        reqID1 = ((((str(bankName) + '.') + str(self.cId)) + '.') + str(self.seq))
        bankIndex = [x for x in self.bankList if (x.name == bankName)]
        self.output(('Client: Sending getBalance ' + reqID1))
        i = resend
        while i:
            self.psend('BALANCE', reqID1, accountNum, 0, bankIndex[0].tail)

            def ExistentialOpExpr_0():
                for (_, _, (_ConstantPattern28_, _BoundPattern29_, _, _, _)) in self._ClientReceivedEvent_3:
                    if (_ConstantPattern28_ == 'REPLY'):
                        if (_BoundPattern29_ == reqID1):
                            if True:
                                return True
                return False
            _st_label_42 = 0
            self._timer_start()
            while (_st_label_42 == 0):
                _st_label_42 += 1
                if ExistentialOpExpr_0():
                    self.output(('Client: get balance response received for ' + str(reqID1)))
                    _st_label_42 += 1
                elif self._timer_expired:
                    nretries = (nretries - 1)
                    if (nretries > 0):
                        self.output(((((('Client: Resending for request Id ' + str(bankName)) + '.') + str(self.cId)) + '.') + str(self.seq)))
                        self.getBalance(resend, bankName, accountNum, nretries)
                    else:
                        self.output('Client: maximun Retry count for reqId')
                        pass
                    _st_label_42 += 1
                else:
                    super()._label('_st_label_42', block=True, timeout=self.T)
                    _st_label_42 -= 1
            else:
                if (_st_label_42 != 2):
                    continue
            if (_st_label_42 != 2):
                break
            i = (i - 1)

    def deposit(self, resend, bankName, accountNum, amount, nretries):
        if (nretries == self.tryC):
            self.seq = (self.seq + 1)
        reqID2 = ((((str(bankName) + '.') + str(self.cId)) + '.') + str(self.seq))
        bankIndex = [x for x in self.bankList if (x.name == bankName)]
        self.output(((('Client: Sending Deposit ' + reqID2) + ' ') + str(amount)))
        i = resend
        while i:
            self.psend('DEPOSIT', reqID2, accountNum, amount, bankIndex[0].head)

            def ExistentialOpExpr_1():
                for (_, _, (_ConstantPattern46_, _BoundPattern47_, _, _, _)) in self._ClientReceivedEvent_4:
                    if (_ConstantPattern46_ == 'REPLY'):
                        if (_BoundPattern47_ == reqID2):
                            if True:
                                return True
                return False
            _st_label_60 = 0
            self._timer_start()
            while (_st_label_60 == 0):
                _st_label_60 += 1
                if ExistentialOpExpr_1():
                    self.output(('Client: deposit received for ' + reqID2))
                    _st_label_60 += 1
                elif self._timer_expired:
                    nretries = (nretries - 1)
                    if (nretries > 0):
                        self.output(((((('Client: Resending for request Id ' + str(bankName)) + '.') + str(self.cId)) + '.') + str(self.seq)))
                        self.deposit(resend, bankName, accountNum, amount, nretries)
                    else:
                        self.output('Client: maximum retry count reached for deposit')
                        pass
                    _st_label_60 += 1
                else:
                    super()._label('_st_label_60', block=True, timeout=self.T)
                    _st_label_60 -= 1
            else:
                if (_st_label_60 != 2):
                    continue
            if (_st_label_60 != 2):
                break
            i = (i - 1)

    def withdraw(self, resend, bankName, accountNum, amount, nretries):
        if (nretries == self.tryC):
            self.seq = (self.seq + 1)
        reqID = ((((str(bankName) + '.') + str(self.cId)) + '.') + str(self.seq))
        bankIndex = [x for x in self.bankList if (x.name == bankName)]
        self.output(((('Client: Sending withdraw ' + reqID) + ' ') + str(amount)))
        i = resend
        while i:
            self.psend('WITHDRAW', reqID, accountNum, amount, bankIndex[0].head)

            def ExistentialOpExpr_2():
                for (_, _, (_ConstantPattern64_, _BoundPattern65_, _, _, _)) in self._ClientReceivedEvent_5:
                    if (_ConstantPattern64_ == 'REPLY'):
                        if (_BoundPattern65_ == reqID):
                            if True:
                                return True
                return False
            _st_label_78 = 0
            self._timer_start()
            while (_st_label_78 == 0):
                _st_label_78 += 1
                if ExistentialOpExpr_2():
                    self.output(('Client: withdraw received for ' + str(reqID)))
                    _st_label_78 += 1
                elif self._timer_expired:
                    nretries = (nretries - 1)
                    if (nretries > 0):
                        self.output(((((('Client: Resending for request Id ' + str(bankName)) + '.') + str(self.cId)) + '.') + str(self.seq)))
                        self.withdraw(resend, bankName, accountNum, amount, nretries)
                    else:
                        self.output('Client: maximum retry count reached for withdraw')
                        pass
                    _st_label_78 += 1
                else:
                    super()._label('_st_label_78', block=True, timeout=self.T)
                    _st_label_78 -= 1
            else:
                if (_st_label_78 != 2):
                    continue
            if (_st_label_78 != 2):
                break
            i = (i - 1)

    def transfer(self, resend, bankName, accountNum, amount, destBank, destAccount, nretries):
        if (nretries == self.tryC):
            self.seq = (self.seq + 1)
        reqID = ((((str(bankName) + '.') + str(self.cId)) + '.') + str(self.seq))
        i = resend
        while i:
            bankIndex = [x for x in self.bankList if (x.name == bankName)]
            self.output(((('Client: Sending transfer ' + reqID) + ' ') + str(amount)))
            self._send(('TRANSFER', reqID, accountNum, amount, destBank, destAccount), bankIndex[0].head)
            result = None

            def ExistentialOpExpr_3():
                nonlocal result
                for (_, _, (_ConstantPattern82_, _BoundPattern83_, result, _, _)) in self._ClientReceivedEvent_6:
                    if (_ConstantPattern82_ == 'REPLY'):
                        if (_BoundPattern83_ == reqID):
                            if True:
                                return True
                return False
            _st_label_96 = 0
            self._timer_start()
            while (_st_label_96 == 0):
                _st_label_96 += 1
                if ExistentialOpExpr_3():
                    self.output(((('Client: transfer received for ' + str(reqID)) + ' result ') + str(result)))
                    _st_label_96 += 1
                elif self._timer_expired:
                    nretries = (nretries - 1)
                    if (nretries > 0):
                        self.output(((((('Client: Resending for request Id ' + str(bankName)) + '.') + str(self.cId)) + '.') + str(self.seq)))
                        self.transfer(resend, bankName, accountNum, amount, destBank, destAccount, nretries)
                    else:
                        self.output('Client: maximum retry count reached for transfer')
                        pass
                    _st_label_96 += 1
                else:
                    super()._label('_st_label_96', block=True, timeout=20)
                    _st_label_96 -= 1
            else:
                if (_st_label_96 != 2):
                    continue
            if (_st_label_96 != 2):
                break
            i = (i - 1)

    def psend(self, type, reqId, accountNum, amount, recepient):
        prob = random.random()
        if (prob >= self.dropRate):
            self._send((type, reqId, accountNum, amount), recepient)
        else:
            self.output(('Client: Dropping send message with ReqID ' + reqId))

    def _Client_handler_0(self, balance, accountNumber, outcome, reqID):
        self.responses.append((reqID, balance))
    _Client_handler_0._labels = None
    _Client_handler_0._notlabels = None

    def _Client_handler_1(self, head, bankName, master):
        bankIndex = [x for x in self.bankList if (x.name == bankName)]
        bankIndex[0].head = head
    _Client_handler_1._labels = None
    _Client_handler_1._notlabels = None

    def _Client_handler_2(self, tail, bankName):
        bankIndex = [x for x in self.bankList if (x.name == bankName)]
        bankIndex[0].tail = tail
    _Client_handler_2._labels = None
    _Client_handler_2._notlabels = None

def main(master, fileName):
    json_data = open(fileName)
    info = json.load(json_data)
    bankList = []
    bank_client_list = []
    bank_client_dict = {}
    for bank in info['master']['banks']:
        temp = Bank(bank['bank_name'], None, None)
        bankList.append(temp)
    for bank in info['master']['banks']:
        bank_client_list = da.api.new(Client, num=bank['bank_clients'])
        i = 0
        for cli in bank_client_list:
            da.api.setup(cli, [bank['clients'][i]['client_id'], bank['bank_name'], master, bankList, bank['clients'][i]['client_total_req'], bank['clients'][i]['client_prob_getbal'], bank['clients'][i]['client_prob_dep'], bank['clients'][i]['client_prob_with'], bank['clients'][i]['client_requests'], bank['clients'][i]['client_drop_rate']])
            i += 1
        bank_client_dict[bank['bank_name']] = bank_client_list
    for bank in bank_client_dict:
        da.api.start(bank_client_dict[bank])
