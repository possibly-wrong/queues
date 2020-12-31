from simpy import Event, Simulation
from collections import deque
from random import randint, expovariate

num_servers = 3
arrival_time = 2
service_time = 5
end_time = 40 * 50 * 40 * 60 # 40 years, 50 weeks/year, 40 hours/week

wait_times = []
queue = [deque() for server in range(num_servers)]

def arrival(sim, event):
    sizes = [len(queue[server]) for server in range(num_servers)]
    server = sizes.index(min(sizes))
    #server = randint(0, num_servers - 1) # pick random queue
    if not queue[server]:
        sim.publish(Event(type='BeginService', data=server,
                          time=event.time))
    queue[server].append(event.time)
    sim.publish(Event(type='Arrival',
                      time=event.time + expovariate(1 / arrival_time)))

def begin_service(sim, event):
    server = event.data
    wait_times.append(event.time - queue[server][0])
    sim.publish(Event(type='EndService', data=server,
                      time=event.time + expovariate(1 / service_time)))
    
def end_service(sim, event):
    server = event.data
    queue[server].popleft()
    if queue[server]:
        sim.publish(Event(type='BeginService', data=server,
                          time=event.time))

sim = Simulation()
sim.subscribe('Arrival', arrival)
sim.subscribe('BeginService', begin_service)
sim.subscribe('EndService', end_service)
sim.publish(Event(type='Arrival', time=expovariate(1 / arrival_time)))
sim.stop_at(end_time)
sim.run()
print(sum(wait_times) / len(wait_times))
