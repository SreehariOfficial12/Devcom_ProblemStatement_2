import random
import datetime
import uuid

# List of data keys used in the 'data' field of the records
_DATA_KEYS = ["a", "b", "c"]

class Device:
    def __init__(self, id):
        # Constructor for the Device class
        self._id = id  # Device ID
        self.records = []  # List to store records received from the SyncService
        self.sent = []  # List to store records sent by the device

    def obtainData(self) -> dict:
        """Returns a single new datapoint from the device."""
        if random.random() < 0.4:
            # Sometimes there's no new data
            return {}

        # Generate a record with a timestamp and random data values
        rec = {
            'type': 'record',
            'timestamp': datetime.datetime.now().isoformat(),
            'dev_id': self._id,
            'data': {kee: str(uuid.uuid4()) for kee in _DATA_KEYS}
        }
        self.sent.append(rec)  # Store the record in the 'sent' list
        return rec

    def probe(self) -> dict:
        """Returns a probe request to be sent to the SyncService."""
        if random.random() < 0.5:
            # Sometimes the device forgets to probe the SyncService
            return {}

        # Create a probe request indicating the index from which data is requested
        return {'type': 'probe', 'dev_id': self._id, 'from': len(self.records)}

    def onMessage(self, data: dict):
        """Receives updates from the server."""
        if random.random() < 0.6:
            # Sometimes devices make mistakes. Let's hope the SyncService handles such failures.
            return

        if data['type'] == 'update':
            _from = data['from']
            if _from > len(self.records):
                return
            self.records = self.records[:_from] + data['data']

class SyncService:
    def __init__(self):
        # Initialize the SyncService with an empty list to store server records
        self.server_records = []

    def onMessage(self, data: dict):
        """Handle messages received from devices."""
        if 'type' not in data:
            # Handle the case where 'type' key is not present in data
            return None

        if data['type'] == 'probe':
            from_index = data.get('from', 0)
            response_data = self.server_records[from_index:]
            # Respond with an 'update' containing the requested data
            return {'type': 'update', 'from': from_index, 'data': response_data}
        elif data['type'] == 'record':
            # Store received records in the server_records list
            self.server_records.append(data['data'])

def testSyncing():
    # Create 10 devices and a SyncService
    devices = [Device(f"dev_{i}") for i in range(10)]
    syn = SyncService()

    # Run a large number of iterations to simulate device interactions
    _N = int(1e6)
    for i in range(_N):
        for _dev in devices:
            # Simulate obtaining data, sending data, and probing the SyncService
            syn.onMessage(_dev.obtainData())
            _dev.onMessage(syn.onMessage(_dev.probe()))

    done = False
    while not done:
        for _dev in devices:
            # Continue probing and updating until synchronization is complete
            _dev.onMessage(syn.onMessage(_dev.probe()))
        num_recs = len(devices[0].records)
        done = all([len(_dev.records) == num_recs for _dev in devices])

    ver_start = [0] * len(devices)
    for i, rec in enumerate(devices[0].records):
        # Verify that the received records match the expected records
        _dev_idx = int(rec['dev_id'].split("_")[-1])
        assertEquivalent(rec, devices[_dev_idx].sent[ver_start[_dev_idx]])
        for _dev in devices[1:]:
            assertEquivalent(rec, _dev.records[i])
        ver_start[_dev_idx] += 1

def assertEquivalent(d1: dict, d2: dict):
    # Compare the values of the 'dev_id', 'timestamp', and 'data' fields
    assert d1['dev_id'] == d2['dev_id']
    assert d1['timestamp'] == d2['timestamp']
    for kee in _DATA_KEYS:
        assert d1['data'][kee] == d2['data'][kee]

# Testing the synchronization
testSyncing()
