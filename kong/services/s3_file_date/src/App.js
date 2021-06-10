import React, { useState } from 'react'
import * as dateService from "./services/date";

function App() {
  
  const [ date, setDate ] = useState('')


  const handlerDateTrackerChange = (event) => {
    setDate(event.target.value)
  }

  const handlerTracker = (event) => {
    dateService.putDate(date)
    setDate('')
  }
  
  return (
    <div className="App">
        <label name="start"> S3 Date:</label>
        <li>

        
        <input type="date" id="date_file_tracker" name="date_file_tracker" onChange={handlerDateTrackerChange} ></input>
        <input type="button" id="start_file_tracker" onClick={handlerTracker} value="Start"/>
        </li>
    </div>
  );
}

export default App;
