import asyncio
from datetime import datetime, timedelta
from typing import Callable, List

class SchedulerError(Exception):
    """Custom exception for Scheduler-related errors."""
    pass

class Scheduler:
    def __init__(self, next_time_func: Callable[[datetime], datetime], loop: asyncio.AbstractEventLoop = None):
        """
        Initialize the scheduler.
        
        :param next_time_func: A function to calculate the next event time.
        :param loop: Event loop to use for scheduling. Defaults to asyncio.get_event_loop().
        """
        self.loop = loop or asyncio.get_event_loop()
        self.next_time_func = next_time_func
        self.started = False
        self.send_channels: List[asyncio.Queue] = []

    def register(self) -> asyncio.Queue:
        """
        Register a new channel to receive scheduler events.

        :return: An asyncio.Queue that will receive time events.
        :raises SchedulerError: If the scheduler has already started.
        """
        if self.started:
            raise SchedulerError("Scheduler already started. Cannot register new channels.")
        
        queue = asyncio.Queue()
        self.send_channels.append(queue)
        return queue

    async def _broadcast_time(self):
        """
        Broadcast time events to all registered channels.
        """
        try:
            next_fire_time = self.next_time_func(datetime.utcnow())
            while True:
                now = datetime.utcnow()
                wait_time = (next_fire_time - now).total_seconds()
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(1)  # Ensure there's no busy-waiting
                
                # Broadcast the current time
                for queue in self.send_channels:
                    await queue.put(next_fire_time)
                
                # Calculate the next fire time
                next_fire_time = self.next_time_func(next_fire_time + timedelta(seconds=1))
        except asyncio.CancelledError:
            pass
        finally:
            # Clean up by closing all queues
            for queue in self.send_channels:
                queue.put_nowait(None)

    def start(self):
        """
        Start the scheduler.
        """
        if self.started:
            return
        self.started = True
        self.broadcast_task = self.loop.create_task(self._broadcast_time())

    def stop(self):
        """
        Stop the scheduler and clean up resources.
        """
        if hasattr(self, "broadcast_task"):
            self.broadcast_task.cancel()

# Utility Functions
def round_up_15_minutes(t: datetime) -> datetime:
    """Round up the time to the nearest 15-minute interval."""
    return (t + timedelta(seconds=449)).replace(second=0, microsecond=0, minute=(t.minute // 15 + 1) * 15)

def round_up_5_minutes(t: datetime) -> datetime:
    """Round up the time to the nearest 5-minute interval."""
    return (t + timedelta(seconds=149)).replace(second=0, microsecond=0, minute=(t.minute // 5 + 1) * 5)
