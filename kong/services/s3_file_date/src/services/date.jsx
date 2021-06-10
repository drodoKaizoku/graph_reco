import axios from "axios";

const client = axios.create({
  baseURL: "http://localhost:3001/",
});

const putDate = (dateTrack) => {
    
    const dateObj = {
        date: dateTrack
    };
    return client.post("/", dateObj);
};

export {putDate}