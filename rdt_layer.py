from segment import Segment
import math


# #################################################################################################################### #
# RDTLayer                                                                                                             #
#                                                                                                                      #
# Description:                                                                                                         #
# The reliable data transfer (RDT) layer is used as a communication layer to resolve issues over an unreliable         #
# channel.                                                                                                             #
#                                                                                                                      #
#                                                                                                                      #
# Notes:                                                                                                               #
# This file is meant to be changed.                                                                                    #
#                                                                                                                      #
#                                                                                                                      #
# #################################################################################################################### #

DATA_LENGTH = 4
FLOW_CONTROL_WIN_SIZE = 15 # in characters          # Receive window size for flow-control

class RDTLayer(object):
    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.currentIteration = 0
        # Add items as needed
        self.seq = 0
        self.countSegmentTimeouts = 0
        self.last_proc_byte = 0         # Sending next or resend logic
        self.send_queue = []            # For maintaining flow controls
        self.staged_segments = []       # Segments received out of order, awaiting processing
        self.rcvd_ack = []              # Counting Acks, if freq > 3 it will be resent
        self.waiting_ack = []           # Seq numbers sent and awaiting Ack
        self.captured_segments = []     # Segments processed. For final string compare

    # ################################################################################################################ #
    # setSendChannel()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable sending lower-layer channel                                                 #                                                                                                                #
    # ################################################################################################################ #
    def setSendChannel(self, channel):
        self.sendChannel = channel

    # ################################################################################################################ #
    # setReceiveChannel()                                                                                              #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the unreliable receiving lower-layer channel                                               #                                                                                                                #
    # ################################################################################################################ #
    def setReceiveChannel(self, channel):
        self.receiveChannel = channel

    # ################################################################################################################ #
    # setDataToSend()                                                                                                  #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to set the string data to send                                                                    #                                                                                                                 #
    # ################################################################################################################ #
    def setDataToSend(self,data):
        self.dataToSend = data

    # ################################################################################################################ #
    # getDataReceived()                                                                                                #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Called by main to get the currently received and buffered string data, in order  
    # Sorts received segments and appends the payload in sequence order                                #                                                                                                                 #
    # ################################################################################################################ #
    def getDataReceived(self):
        result = ''
        segments = sorted(self.captured_segments, key=lambda s: s.seqnum, reverse=False)
        for payload in segments:
            result += payload.payload
        return result

    # ################################################################################################################ #
    # processData()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # "timeslice". Called by main once per iteration                                                                   #                                                                                                                 #
    # ################################################################################################################ #
    def processData(self):
        self.currentIteration += 1
        if len(self.dataToSend) > 0:
            self.processSend()
        self.processReceiveAndSendRespond()

    # ################################################################################################################ #
    # processSend()                                                                                                    #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment sending tasks                                                                                    #                                                                                                                #
    # ################################################################################################################ #
    def processSend(self):
        # You should pipeline segments to fit the flow-control window
        # The flow-control window is the constant RDTLayer.FLOW_CONTROL_WIN_SIZE
        # The maximum data that you can send in a segment is RDTLayer.DATA_LENGTH
        # These constants are given in # characters

        # Somewhere in here you will be creating data segments to send.
        # The data is just part of the entire string that you are trying to send.
        # The seqnum is the sequence number for the segment (in character number, not bytes)

        # Helper function
        def send_data(seq, data):
            segmentSend = Segment()
            segmentSend.setData(seq,data)
            # Stage the segement in the send queue
            self.send_queue.append(segmentSend)

        # If Client has more bytes to send to server
        if (len(self.dataToSend) > 0) and (self.seq < len(self.dataToSend)):
            while len(self.send_queue) <= math.floor(FLOW_CONTROL_WIN_SIZE / DATA_LENGTH):
                data = self.dataToSend[self.seq:self.seq + DATA_LENGTH]
                send_data(self.seq, data)
                self.seq += len(data)

        # Once client sends all segments, resend segments without ack
        elif (len(self.dataToSend) > 0) and (self.seq >= len(self.dataToSend)):
            for seq in self.waiting_ack:
                data = self.dataToSend[seq: seq + DATA_LENGTH]
                send_data(seq, data)

        # Stay within the flow control window size
        for seg in range(math.floor(FLOW_CONTROL_WIN_SIZE / DATA_LENGTH)):
            if len(self.send_queue) > 0:
                # Pop off the first segment and send it
                sendSeg = self.send_queue.pop(0)
                print("Sending segment: ", sendSeg.to_string())
                self.sendChannel.send(sendSeg)
                self.waiting_ack.append(sendSeg.seqnum)

                if sendSeg.seqnum == 0:
                    print("sendSeg.seqnum < seq")

    # ################################################################################################################ #
    # processReceive()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment receive tasks                                                                                    #                                                                                                              #
    # ################################################################################################################ #
    def processReceiveAndSendRespond(self):

        # Helper function to seperate client duties from server duties
        def client_process_recv_ack():
            # Client handling acknowledgemetns received from the server
            sorted_incoming = sorted(self.receiveChannel.receive(), key=lambda s: s.seqnum, reverse=False)
            
            # Append acknoledgements to received list
            for seg in sorted_incoming:
                self.rcvd_ack.append(seg.acknum)
                # Remove acknowledged segments from list of awaiting acknowledgements
                if seg.acknum in self.waiting_ack:
                    # Refrence: https://www.pythonforbeginners.com/basics/list-comprehensions-in-python
                    self.waiting_ack[:] = [x for x in self.waiting_ack if x > seg.acknum]

            # If acknowledgement was received more than 3 times, resend it
            freq = {}
            for item in self.rcvd_ack:
                freq[item] = self.rcvd_ack.count(item)

            for seq, cnt in freq.items():
                if cnt >= 3:
                    data = self.dataToSend[seq: seq + DATA_LENGTH]
                    # Form and send segment
                    segmentSend = Segment()
                    segmentSend.setData(seq,data)
                    # Place the segment in front of the queue
                    self.send_queue = [segmentSend] + self.send_queue

                    # remove resent segment
                    for item in self.rcvd_ack:
                        if item == seq:
                            self.rcvd_ack[:] = [x for x in self.rcvd_ack if x >= item]
                        

        # Helper function to seperate server duties from client duties
        def server_process_recv_ack():
            
            # Helper function, redundant actions
            def send_ack(ack):
                segmentAck = Segment()
                segmentAck.setAck(ack)
                print("Sending ack: ", segmentAck.to_string())
                self.sendChannel.send(segmentAck)

            # Server receiving data
            # This call returns a list of incoming segments (see Segment class)...
            listIncomingSegments = sorted(self.receiveChannel.receive(), key=lambda s: s.seqnum, reverse=False)

            if len(listIncomingSegments) > 0:                
                for segment in range(len(listIncomingSegments)):
                    # Verify checksum
                    if listIncomingSegments[segment].checkChecksum():

                        if listIncomingSegments[segment].seqnum == self.last_proc_byte:
                            # Send ack
                            send_ack(listIncomingSegments[segment].seqnum + len(listIncomingSegments[segment].payload))

                            # Increment last processed byte variable
                            self.last_proc_byte += len(listIncomingSegments[segment].payload)

                            # Store received segments
                            self.captured_segments.append(listIncomingSegments[segment])

                            # Iterate staged segments to increment last_proc_byte up to next highest in-order received seq
                            # Move segements from staged to captured segments
                            for item in sorted(self.staged_segments, key=lambda s: s.seqnum, reverse=False):
                                if item.seqnum == self.last_proc_byte:
                                    self.captured_segments.append(item)
                                    self.staged_segments.remove(item)
                                    self.last_proc_byte += len(item.payload)

                        # Send ack cumulative ack. Append received segement to staged segments list
                        # when segments are missing or corrupted
                        elif listIncomingSegments[segment].seqnum > self.last_proc_byte:
                            send_ack(self.last_proc_byte)
                            self.staged_segments.append(listIncomingSegments[segment])

                        elif listIncomingSegments[segment].seqnum < self.last_proc_byte:
                            continue

                    # Send cumulative ack. 
                    else:
                        send_ack(self.last_proc_byte)


        # Determine server or client mode
        if (len(self.dataToSend) > 0) and (len(self.receiveChannel.receiveQueue) > 0):
            client_process_recv_ack()
        else:
            server_process_recv_ack()


