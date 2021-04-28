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
sendChannel = None
receiveChannel = None
dataToSend = ''
currentIteration = 0  


class RDTLayer(object):
    def __init__(self):
        self.sendChannel = None
        self.receiveChannel = None
        self.dataToSend = ''
        self.data_len = 0
        self.currentIteration = 0
        # Add items as needed
        self.seq = 0
        self.dataReceived = ''
        self.countSegmentTimeouts = 0
        self.waiting_ack = []
        self.captured_segments = []
        self.processed_seq = [] 

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
    # Called by main to get the currently received and buffered string data, in order                                  #                                                                                                                 #
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

        if (len(self.dataToSend) > 0) and (self.seq < len(self.dataToSend)):
            
            for seg in range(math.floor(FLOW_CONTROL_WIN_SIZE / DATA_LENGTH)):
                segmentSend = Segment()
                data = self.dataToSend[self.seq:self.seq + DATA_LENGTH]

                # ############################################################################################################ #
                # Display sending segment
                segmentSend.setData(self.seq,data)
                # print("Sending segment: ", segmentSend.to_string())

                # Use the unreliable sendChannel to send the segment
                self.sendChannel.send(segmentSend)

                # Increment seq number
                self.waiting_ack.append(self.seq)
                self.seq += DATA_LENGTH
        elif (len(self.dataToSend) > 0) and (self.seq > len(self.dataToSend)):
            # Resend packets which have not been acknowledged
            # resent = []
            for seq in self.waiting_ack:
                # Get data to resend
                start = seq 
                end = seq + DATA_LENGTH
                data = self.dataToSend[start: end]

                # Form then send a segment
                segmentSend = Segment()
                segmentSend.setData(seq, data)
                # print("Resending segment: ", segmentSend.to_string())
                self.sendChannel.send(segmentSend)

        else:
            # No data to send, in acknowledgement cycle
            pass

    # ################################################################################################################ #
    # processReceive()                                                                                                 #
    #                                                                                                                  #
    # Description:                                                                                                     #
    # Manages Segment receive tasks                                                                                    #                                                                                                              #
    # ################################################################################################################ #
    def processReceiveAndSendRespond(self):

        if len(self.dataToSend) > 0:
            # Client handling acknowledgemetns received from the server
            if len(self.receiveChannel.receiveQueue) > 0:
                sorted_incoming = sorted(self.receiveChannel.receive(), key=lambda s: s.seqnum, reverse=False)
                
                # Remove acknowledged segments from list of acknowledgements
                for seg in sorted_incoming:
                    # print("Client received ask for: ", seg.acknum)
                    if seg.acknum in self.waiting_ack:
                        self.waiting_ack.remove(seg.acknum) 

                # Resend packets which have not been acknowledged
                resent = []
                for seq in self.waiting_ack:
                    if seq < (self.seq - (DATA_LENGTH * 5)):
                        # Get data to resend
                        start = seq 
                        end = seq + DATA_LENGTH
                        data = self.dataToSend[start: end]

                        # Form then send a segment
                        segmentSend = Segment()
                        segmentSend.setData(seq, data)
                        # print("Resending segment: ", segmentSend.to_string())
                        self.sendChannel.send(segmentSend)

                        # Remove, but keep track of segments resent
                        self.waiting_ack.remove(seq)
                        resent.append(seq)
                # Add resemt segments to wait ack list
                for x in resent:
                    self.waiting_ack.append(x)
                resent.clear()                
            else:
                pass

        else:
            # Server receiving data

            # This call returns a list of incoming segments (see Segment class)...
            listIncomingSegments = sorted(self.receiveChannel.receive(), key=lambda s: s.seqnum, reverse=False)
            inc_seg_len = len(listIncomingSegments)

            # ############################################################################################################ #
            # What segments have been received?
            # How will you get them back in order?
            # This is where a majority of your logic will be implemented
            if inc_seg_len == 0:
                pass

            elif inc_seg_len > 0:
                # Collect corrupted segments to a list
                discard = []
                for segment in listIncomingSegments:
                    testSegment = Segment()
                    testSegment.setData(segment.seqnum, segment.payload)

                    if (testSegment.checksum != segment.checksum): # and (len(segment.payload) > 0):
                        discard.append(segment)
                # Remove corrupted segments from incoming segment list
                for corrupted in discard:
                    listIncomingSegments.remove(corrupted)
                
                for segment in range(len(listIncomingSegments)):
                    # If the packet has not been processed yet
                    if listIncomingSegments[segment].seqnum not in self.processed_seq:

                        # Segment acknowledging packet(s) received
                        segmentAck = Segment()
                        segmentAck.setAck(listIncomingSegments[segment].seqnum)
                        # print("Sending ack: ", segmentAck.to_string(), " ", listIncomingSegments[segment].payload)

                        # Use the unreliable sendChannel to send the ack packet
                        self.sendChannel.send(segmentAck)
                        self.captured_segments.append(listIncomingSegments[segment])
                        self.processed_seq.append(listIncomingSegments[segment].seqnum)

                    # Acknowledge previously processed, duplicate segment    
                    else:
                        segmentAck = Segment()     # Segment acknowledging packet(s) received
                        segmentAck.setAck(listIncomingSegments[segment].seqnum)
                        # print("Sending ack: ", segmentAck.to_string(), " ", listIncomingSegments[segment].payload)
                        self.sendChannel.send(segmentAck)


