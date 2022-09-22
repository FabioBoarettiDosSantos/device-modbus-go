// -*- Mode: Go; indent-tabs-mode: t -*-
//
// Copyright (C) 2018-2021 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

// Package driver is used to execute device-sdk's commands
package driver

import (
	"fmt"
	"sync"
	"time"

	sdkModel "github.com/edgexfoundry/device-sdk-go/v2/pkg/models"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v2/models"
)

var once sync.Once
var driver *Driver

type Register struct {
	value []byte
}

type Driver struct {
	Logger              logger.LoggingClient
	AsyncCh             chan<- *sdkModel.AsyncValues
	mutex               sync.Mutex
	addressMap          map[string]chan bool
	workingAddressCount map[string]int
	stopped             bool
}

var concurrentCommandLimit = 100

var registerMap = make(map[uint16]Register)

func (d *Driver) DisconnectDevice(deviceName string, protocols map[string]models.ProtocolProperties) error {
	d.Logger.Warn("Driver's DisconnectDevice function didn't implement")
	return nil
}

// lockAddress mark address is unavailable because real device handle one request at a time
func (d *Driver) lockAddress(address string) error {
	if d.stopped {
		return fmt.Errorf("service attempts to stop and unable to handle new request")
	}
	d.mutex.Lock()
	lock, ok := d.addressMap[address]
	if !ok {
		lock = make(chan bool, 1)
		d.addressMap[address] = lock
	}

	// workingAddressCount used to check high-frequency command execution to avoid goroutine block
	count, ok := d.workingAddressCount[address]
	if !ok {
		d.workingAddressCount[address] = 1
	} else if count >= concurrentCommandLimit {
		d.mutex.Unlock()
		errorMessage := fmt.Sprintf("High-frequency command execution. There are %v commands with the same address in the queue", concurrentCommandLimit)
		d.Logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	} else {
		d.workingAddressCount[address] = count + 1
	}

	d.mutex.Unlock()
	lock <- true

	return nil
}

// unlockAddress remove token after command finish
func (d *Driver) unlockAddress(address string) {
	d.mutex.Lock()
	lock := d.addressMap[address]
	d.workingAddressCount[address] = d.workingAddressCount[address] - 1
	d.mutex.Unlock()
	<-lock
}

// lockableAddress return the lockable address according to the protocol
func (d *Driver) lockableAddress(info *ConnectionInfo) string {
	var address string
	if info.Protocol == ProtocolTCP {
		address = fmt.Sprintf("%s:%d", info.Address, info.Port)
	} else {
		address = info.Address
	}
	return address
}

func (d *Driver) HandleReadCommands(deviceName string, protocols map[string]models.ProtocolProperties, reqs []sdkModel.CommandRequest) (responses []*sdkModel.CommandValue, err error) {
	connectionInfo, err := createConnectionInfo(protocols)
	if err != nil {
		driver.Logger.Errorf("Fail to create read command connection info. err:%v \n", err)
		return responses, err
	}

	err = d.lockAddress(d.lockableAddress(connectionInfo))
	if err != nil {
		return responses, err
	}
	defer d.unlockAddress(d.lockableAddress(connectionInfo))

	responses = make([]*sdkModel.CommandValue, len(reqs))
	var deviceClient DeviceClient

	// create device client and open connection
	//deviceClient, err = NewDeviceClient(connectionInfo)
	//if err != nil {
	//	driver.Logger.Infof("Read command NewDeviceClient failed. err:%v \n", err)
	//	return responses, err
	//}

	//err = deviceClient.OpenConnection()
	//if err != nil {
	//	driver.Logger.Infof("Read command OpenConnection failed. err:%v \n", err)
	//	return responses, err
	//}

	//defer func() { _ = deviceClient.CloseConnection() }()

	// handle command requests
	for i, req := range reqs {

		commandInfo, err := createCommandInfo(&req)
		if err != nil {
			return nil, err
		}
		_, mapPresent := registerMap[commandInfo.StartingAddress]

		if (!mapPresent) || (commandInfo.PushValueToMap) {

			driver.Logger.Infof("WILL CREATE A CONNECTION....... Register: %v  MapPresent:%b   PushToMap:%b", commandInfo.StartingAddress, mapPresent, commandInfo.PushValueToMap)
			deviceClient, err = NewDeviceClient(connectionInfo)
			if err != nil {
				driver.Logger.Infof("Read command NewDeviceClient failed. err:%v \n", err)
				return responses, err
			}

			err = deviceClient.OpenConnection()
			if err != nil {
				driver.Logger.Infof("Read command OpenConnection failed. err:%v \n", err)
				return responses, err
			}

			defer func() { _ = deviceClient.CloseConnection() }()
		}

		res, err := handleReadCommandRequest(deviceClient, req)
		if err != nil {
			//driver.Logger.Infof("Read command failed. Cmd:%v err:%v \n", req.DeviceResourceName, err)
			return responses, err
		}

		responses[i] = res
	}

	return responses, nil
}

func handleReadCommandRequest(deviceClient DeviceClient, req sdkModel.CommandRequest) (*sdkModel.CommandValue, error) {
	var response []byte
	var responseFinal []byte
	var result = &sdkModel.CommandValue{}
	var err error

	commandInfo, err := createCommandInfo(&req)
	if err != nil {
		return nil, err
	}
	driver.Logger.Infof("READ LENGTH : %v \n", commandInfo.Length)
	_, mapPresent := registerMap[commandInfo.StartingAddress]
	if (commandInfo.UseValueFromMap) && (mapPresent) {
		//USE VALUE FROM A MAP OF REGISTERS
		response = registerMap[commandInfo.StartingAddress].value
		//driver.Logger.Infof("Value found on MAP addr : %v,  Bytes: %v \n", commandInfo.StartingAddress, response)

	} else {

		response, err = deviceClient.GetValue(commandInfo)
		if err != nil {
			return result, err
		}

	}

	if commandInfo.PushValueToMap {
		//PUSH VALUE OF REGISTER TO THE MAP OF REGS//
		registerMap[uint16(commandInfo.StartingAddress)] = Register{(response)}
		//driver.Logger.Infof("Value Pushed To MAP addr : %v,  Bytes: %v \n", commandInfo.StartingAddress, response)
	}

	//Get Slice of the data using the MapIndex
	var initArray = (commandInfo.MapDataIndex * 2) - 2
	var endArray = (commandInfo.MapDataIndex * 2)

	responseFinal = response[initArray:]

	driver.Logger.Info(fmt.Sprintf("ArrayIndexs %v / %v = %v ", initArray, endArray, responseFinal))

	result, err = TransformDataBytesToResult(&req, responseFinal, commandInfo)
	driver.Logger.Infof("Value Transformed: %v \n", result)

	if err != nil {
		return result, err
	} else {
		driver.Logger.Infof("Read command finished. Cmd:%v, %v \n", req.DeviceResourceName, result)
	}

	//IT WILL PUSH THE REG VALUE TO THE MAP

	return result, nil
}

func (d *Driver) HandleWriteCommands(deviceName string, protocols map[string]models.ProtocolProperties, reqs []sdkModel.CommandRequest, params []*sdkModel.CommandValue) error {
	connectionInfo, err := createConnectionInfo(protocols)
	if err != nil {
		driver.Logger.Errorf("Fail to create write command connection info. err:%v \n", err)
		return err
	}

	err = d.lockAddress(d.lockableAddress(connectionInfo))
	if err != nil {
		return err
	}
	defer d.unlockAddress(d.lockableAddress(connectionInfo))

	var deviceClient DeviceClient

	// create device client and open connection
	deviceClient, err = NewDeviceClient(connectionInfo)
	if err != nil {
		return err
	}

	err = deviceClient.OpenConnection()
	if err != nil {
		return err
	}

	defer func() { _ = deviceClient.CloseConnection() }()

	// handle command requests
	for i, req := range reqs {
		err = handleWriteCommandRequest(deviceClient, req, params[i])
		if err != nil {
			d.Logger.Error(err.Error())
			break
		}
	}

	return err
}

func handleWriteCommandRequest(deviceClient DeviceClient, req sdkModel.CommandRequest, param *sdkModel.CommandValue) error {
	var err error

	commandInfo, err := createCommandInfo(&req)
	if err != nil {
		return err
	}

	dataBytes, err := TransformCommandValueToDataBytes(commandInfo, param)
	if err != nil {
		return fmt.Errorf("transform command value failed, err: %v", err)
	}

	err = deviceClient.SetValue(commandInfo, dataBytes)
	if err != nil {
		return fmt.Errorf("handle write command request failed, err: %v", err)
	}

	driver.Logger.Infof("Write command finished. Cmd:%v \n", req.DeviceResourceName)
	return nil
}

func (d *Driver) Initialize(lc logger.LoggingClient, asyncCh chan<- *sdkModel.AsyncValues, deviceCh chan<- []sdkModel.DiscoveredDevice) error {
	d.Logger = lc
	d.AsyncCh = asyncCh
	d.addressMap = make(map[string]chan bool)
	d.workingAddressCount = make(map[string]int)
	return nil
}

func (d *Driver) Stop(force bool) error {
	d.stopped = true
	if !force {
		d.waitAllCommandsToFinish()
	}
	for _, locked := range d.addressMap {
		close(locked)
	}
	return nil
}

// waitAllCommandsToFinish used to check and wait for the unfinished job
func (d *Driver) waitAllCommandsToFinish() {
loop:
	for {
		for _, count := range d.workingAddressCount {
			if count != 0 {
				// wait a moment and check again
				time.Sleep(time.Second * SERVICE_STOP_WAIT_TIME)
				continue loop
			}
		}
		break loop
	}
}

func (d *Driver) AddDevice(deviceName string, protocols map[string]models.ProtocolProperties, adminState models.AdminState) error {
	d.Logger.Debugf("Device %s is added", deviceName)
	return nil
}

func (d *Driver) UpdateDevice(deviceName string, protocols map[string]models.ProtocolProperties, adminState models.AdminState) error {
	d.Logger.Debugf("Device %s is updated", deviceName)
	return nil
}

func (d *Driver) RemoveDevice(deviceName string, protocols map[string]models.ProtocolProperties) error {
	d.Logger.Debugf("Device %s is removed", deviceName)
	return nil
}

func NewProtocolDriver() sdkModel.ProtocolDriver {
	once.Do(func() {
		driver = new(Driver)
	})
	return driver
}
