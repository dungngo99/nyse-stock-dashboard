import React, { useState } from 'react';
import {
    MDBContainer,
    MDBRow,
    MDBCol
} from 'mdb-react-ui-kit';


import SearchBar from '../components/SearchBar.js';
import StockTable from '../components/StockTable.js';
import NewsBoard from '../components/NewsBoard.js';
import Movers from '../components/Movers.js';
import api from '../utils/api'

export default function Trending() {
    const [trending, setTrending] = useState({})
    const [top5, setTop5] = useState({})
    const [news, setNews] = useState([])
    
    React.useEffect(() => {
        api.get('trending', {
            method: 'GET'
        }).then(
            res => setTrending(res.data)
        ).catch(
            e => console.log(e)
        )

        api.get('movers', {
            method: 'GET'
        }).then(
            res => setTop5(res.data)
        ).catch(
            e => console.log(e)
        )

        api.get('news', {
            method: "GET",
        }).then(
            res => setNews(res.data)
        ).catch(
            e => console.log(e)
        )

    }, []);

    return (
        <MDBContainer fluid >
            <SearchBar></SearchBar>
            <MDBRow>
                <MDBCol md='9' className='d-flex flex-column align-items-between'>
                    <StockTable trending={trending}></StockTable>
                    <Movers top5={top5} ></Movers>
                </MDBCol>

                <MDBCol md='3'>
                    <NewsBoard news={news}></NewsBoard>
                </MDBCol>
            </MDBRow>
        </MDBContainer>
    )
};