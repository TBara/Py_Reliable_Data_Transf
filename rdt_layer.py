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
        self.waiting_ack = []
        self.captured_segments = []
        self.processed_seq = [] # Track of processed seq numbers. Discard duplicate segments

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
            print("Sending segment: ", segmentSend.to_string())
            self.sendChannel.send(segmentSend)

        # If Client has more bytes to send to server
        if (len(self.dataToSend) > 0) and (self.seq < len(self.dataToSend)):
            
            # Stay within the flow control window size
            for seg in range(math.floor(FLOW_CONTROL_WIN_SIZE / DATA_LENGTH)):
                data = self.dataToSend[self.seq:self.seq + DATA_LENGTH]
                send_data(self.seq, data)

                self.waiting_ack.append(self.seq)
                self.seq += DATA_LENGTH

        # Once client sent all segments, resend segments without ack
        elif (len(self.dataToSend) > 0) and (self.seq > len(self.dataToSend)):
            for seq in self.waiting_ack:
                data = self.dataToSend[seq: seq + DATA_LENGTH]
                send_data(seq, data)


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
            
            # Remove acknowledged segments from list of acknowledgements
            for seg in sorted_incoming:
                if seg.acknum in self.waiting_ack:
                    self.waiting_ack.remove(seg.acknum) 

            # Resend packets which have not been acknowledged
            for seq in self.waiting_ack:
                # Timeout counter. Resend segments that have been
                # waitng for acknowledgement for timeout period
                if seq < (self.seq - (DATA_LENGTH * 5)):
                    # Get data to resend
                    data = self.dataToSend[seq: seq + DATA_LENGTH]

                    # Form and send segment
                    segmentSend = Segment()
                    segmentSend.setData(seq, data)
                    print("Resending segment: ", segmentSend.to_string())
                    self.sendChannel.send(segmentSend)

        # Helper function to seperate server duties from client duties
        def server_process_recv_ack():
            # CumulativeAck 11

            # Server receiving data
            # This call returns a list of incoming segments (see Segment class)...
            listIncomingSegments = sorted(self.receiveChannel.receive(), key=lambda s: s.seqnum, reverse=False)

            if len(listIncomingSegments) > 0:                
                for segment in range(len(listIncomingSegments)):
                    if listIncomingSegments[segment].checkChecksum():
                    
                        segmentAck = Segment()     # Segment acknowledging packet(s) received
                        segmentAck.setAck(listIncomingSegments[segment].seqnum)
                        print("Sending ack: ", segmentAck.to_string(), " ", listIncomingSegments[segment].payload)
                        
                        # Use the unreliable sendChannel to send the ack packet
                        self.sendChannel.send(segmentAck)
                        
                        # Store received segments if new, otherwise discard duplicate segments
                        if listIncomingSegments[segment].seqnum not in self.processed_seq:
                            self.captured_segments.append(listIncomingSegments[segment])
                            self.processed_seq.append(listIncomingSegments[segment].seqnum)

        # Determine server or client mode
        if (len(self.dataToSend) > 0) and (len(self.receiveChannel.receiveQueue) > 0):
            client_process_recv_ack()
        else:
            server_process_recv_ack()


