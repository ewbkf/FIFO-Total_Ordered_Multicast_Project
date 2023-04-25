#!/bin/python3

# Author name: Ethan Wharton
import queue

from simulator import *

# Multicast events for the driver
multicast_events = [
    # (time, message_id, sending host_id, message/payload)
    (10, 'M1', 1, 'January'),
    (20, 'M2', 1, 'February'),
    (30, 'M3', 1, 'March'),
    (10, 'M4', 2, 'One'),
    (20, 'M5', 2, 'Two'),
    (30, 'M6', 2, 'Three')
]


class Host(Node):
    def __init__(self, sim, host_id):
        Node.__init__(self, sim, host_id)
        self.host_id = host_id
        self.gmembers = []
        self.hold_back_queue = {}
        self.sequence_id = None
        self.last_id_received = {}

    def initialize(self):
        # TODO: Initialize any data structure or state
        self.sequence_id = 0
        # Message IDs from q will be stored in a dictionary, keys will be group members and values will be last ID rec.
        #   They will each start at 0.
        for q in self.gmembers:
            self.last_id_received[q] = 0
        for q in self.gmembers:
            self.hold_back_queue[q] = queue.PriorityQueue()

    def multicast(self, time, message_id, message_type, payload):
        # Multicast message to the group
        print(f'Time {time}:: {self} SENDING multicast message [{message_id}]')
        # Create message and send to all members of the group including itself
        # TODO: put sequence numbers and so other things
        # Sequence number is incremented for next message
        self.sequence_id += 1
        # Message will be a tuple with the following structure: (sequence_id, message)

        for to in self.gmembers:
            mcast = (self.sequence_id, Message(message_id, self, to, message_type, payload))
            self.send_message(to, mcast)

    def receive_message(self, frm, message, time):
        # This function is called when a message is received by this host (self)
        # frm --- from which host/node the message is came (source of the message)
        # message -- message that is received
        # time -- the time when the message is received (the current time)
        # TODO: Currently this simply delivers the received messages right away, does not implement any ordering
        # TODO: You need to implement your ordering code here

        (sender_sequence_id, message) = message
        print(f'Time {time}:: {self} RECEIVED message [{message.message_id}] from {frm}')
        # Code your stuff

        # Deliver message when it is time
        self.hold_back_queue[frm].put((sender_sequence_id, message))
        while not self.hold_back_queue[frm].empty():
            message = self.hold_back_queue[frm].get()
            (sender_sequence_id, message) = message
            if sender_sequence_id == self.last_id_received[frm] + 1:
                # If S = R + 1 then deliver the message
                self.deliver_message(time, message)
                # S = R
                self.last_id_received[frm] = sender_sequence_id
            elif sender_sequence_id > self.last_id_received[frm] + 1:
                # If S > R + 1, then place the message on the hold back queue
                self.hold_back_queue[frm].put((sender_sequence_id, message))
                break
            else:
                # If a sequence_id is somehow lower than the last one on record, then somehow a message was delivered
                #   out of order.
                print("Error: FIFO ordering violated")
                break
    
    def deliver_message(self, time, message):
        print(f'Time {time:4}:: {self} DELIVERED message [{message.message_id}] -- {message.payload}')
        

# Driver: you DO NOT need to change anything here
class Driver:
    def __init__(self, sim):
        self.hosts = []
        self.sim = sim

    def run(self, nhosts=3):
        for i in range(nhosts):
            host = Host(self.sim, i)
            self.hosts.append(host)
        
        for host in self.hosts:
            host.gmembers = self.hosts
            host.initialize()

        for event in multicast_events:
            time = event[0]
            message_id = event[1]
            message_type = 'DRIVER_MCAST'
            host_id = event[2]
            payload = event[3]
            self.sim.add_event(Event(time, self.hosts[host_id].multicast, time, message_id, message_type, payload))


def main():
    # Create simulation instance
    sim = Simulator(debug=False, random_seed=1233)

    # Start the driver and run for nhosts (should be >= 3)
    driver = Driver(sim)
    driver.run(nhosts=5)

    # Start the simulation
    sim.run()                 


if __name__ == "__main__":
    main()    
