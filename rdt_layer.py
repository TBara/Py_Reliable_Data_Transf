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
        self.currentIteration = 0
        # Add items as needed
        self.seq = 0
        self.dataReceived = ''
        self.countSegmentTimeouts = 0
        self.waiting_ack = []
        self.last_seq_rcvd = 0

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
        return self.dataReceived

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

        if self.seq < len(self.dataToSend):

            # Reset the seq number to the highest char position received by the server so far. If all was received 
            # correctly then this should not change. If packet was missing, the transmission will start at the 
            # missing packet.
            if (len(self.receiveChannel.receiveQueue) > 0) and (self.receiveChannel.receiveQueue[0].acknum >= 0):
                self.seq = self.receiveChannel.receiveQueue[len(self.receiveChannel.receiveQueue) -1].acknum
            
            for seg in range(math.floor(FLOW_CONTROL_WIN_SIZE / DATA_LENGTH)):
                segmentSend = Segment()
                data = self.dataToSend[self.seq:self.seq + DATA_LENGTH]

                # ############################################################################################################ #
                # Display sending segment
                segmentSend.setData(self.seq,data)
                print("Sending segment: ", segmentSend.to_string())

                # Use the unreliable sendChannel to send the segment
                self.sendChannel.send(segmentSend)

                # Increment seq number
                self.seq += DATA_LENGTH
                self.waiting_ack.append(self.seq)

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
        segmentAck = Segment()                  # Segment acknowledging packet(s) received
        segmentAck.acknum = self.seq

        # This call returns a list of incoming segments (see Segment class)...
        # listIncomingSegments = self.receiveChannel.receive()
        listIncomingSegments = sorted(self.receiveChannel.receive(), key=lambda s: s.seqnum, reverse=False)
        inc_seg_len = len(listIncomingSegments)

        # ############################################################################################################ #
        # What segments have been received?
        # How will you get them back in order?
        # This is where a majority of your logic will be implemented
        if inc_seg_len == 0:
            pass


        elif inc_seg_len > 0:
            # ############################################################################################################ #
            # How do you respond to what you have received?
            # How can you tell data segments apart from ack segemnts?
            for segment in range(len(listIncomingSegments)):
                print("Server will process: ", listIncomingSegments[segment].payload)

            for segment in range(len(listIncomingSegments)):
                # acknum of -1 indicates server processing message packets
                if listIncomingSegments[segment].acknum == -1:

                    # seq number -1 indicates packet went missing. Break transmission
                    if (listIncomingSegments[segment].seqnum >= 0) and (listIncomingSegments[segment].seqnum > self.seq) is not True:

                        # Since client performed checksum on Segment converted to string
                        # a sement needs to reconstructed on the server side to calculate and compare checksums
                        testSegment = Segment()
                        testSegment.setData(self.seq, listIncomingSegments[segment].payload)
                        
                        chck_sum1 = testSegment.checksum
                        chck_sum2 = listIncomingSegments[segment].checksum

                        if chck_sum1 == chck_sum2:

                            self.dataReceived += listIncomingSegments[segment].payload
                            self.seq += len(listIncomingSegments[segment].payload)
                            self.last_seq_rcvd = self.seq
                            segmentAck.acknum = self.seq

                            # ############################################################################################################ #
                            # Display response segment
                            segmentAck.setAck(segmentAck.acknum)
                            # print("Sending ack: ", segmentAck.to_string(), " ", listIncomingSegments[segment].payload)
                        else:
                            pass
                    else:
                        break
                else:
                    pass
                # Use the unreliable sendChannel to send the ack packet
            self.sendChannel.send(segmentAck)



                
            # # Sort the list of incoming segments based on seqnum
            # sorted_listIncomingSegments = sorted(listIncomingSegments, key=lambda s: s.seqnum, reverse=False)

            # seg_cnt = 0
            # data_str = ''
            # char_proc = 0
            # # Iterate sorted list 
            # for segment in range(len(sorted_listIncomingSegments)):
            #     # First segment payload is appended to the final string
            #     if seg_cnt == 0:
            #         data_str += sorted_listIncomingSegments[segment].payload
            #         char_proc += len(sorted_listIncomingSegments[segment].payload)
            #         segmentAck.acknum = self.seq + len(sorted_listIncomingSegments[segment].payload)
            #         self.seq += len(sorted_listIncomingSegments[segment].payload)
            #         seg_cnt += 1

            #     # If current segment seqnum is greater than (prev segment seqnum + DATA_LENGHT)
            #     # then a segment is missing

            #     elif ((seg_cnt > 0) and ((sorted_listIncomingSegments[segment].seqnum - sorted_listIncomingSegments[segment - 1].seqnum)== DATA_LENGTH)):

            #         data_str += sorted_listIncomingSegments[segment].payload
            #         char_proc += len(sorted_listIncomingSegments[segment].payload)
            #         segmentAck.acknum = self.seq + len(sorted_listIncomingSegments[segment].payload)
            #         self.seq += len(sorted_listIncomingSegments[segment].payload)
            #         seg_cnt += 1

            # self.dataReceived += data_str
                # self.dataReceived += listIncomingSegments[segment].payload

                # Somewhere in here you will be setting the contents of the ack segments to send.
                # The goal is to employ cumulative ack, just like TCP does...
                # segmentAck.acknum = self.seq + len(listIncomingSegments[segment].payload)
                # self.seq += len(listIncomingSegments[segment].payload)

