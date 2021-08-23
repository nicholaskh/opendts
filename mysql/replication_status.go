/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

// ReplicationStatus holds replication information from SHOW SLAVE STATUS.
type ReplicationStatus struct {
	Position Position
	// RelayLogPosition is the Position that the replica would be at if it
	// were to finish executing everything that's currently in its relay log.
	// However, some MySQL flavors don't expose this information,
	// in which case RelayLogPosition.IsZero() will be true.
	RelayLogPosition     Position
	FilePosition         Position
	FileRelayLogPosition Position
	MasterServerID       uint
	IOThreadRunning      bool
	SQLThreadRunning     bool
	SecondsBehindMaster  uint
	MasterHost           string
	MasterPort           int
	MasterConnectRetry   int
	MasterUUID           SID
}

// ReplicationRunning returns true iff both the IO and SQL threads are
// running.
func (s *ReplicationStatus) ReplicationRunning() bool {
	return s.IOThreadRunning && s.SQLThreadRunning
}
