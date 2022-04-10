import React, { useState } from 'react';
import {
    MDBContainer,
    MDBRow,
    MDBCol
} from 'mdb-react-ui-kit';
import TickerBox from '../components/TickerBox.js';
import SearchBar from '../components/SearchBar.js';
import StockTable from '../components/StockTable.js';
import api from '../utils/api'

export default function Trending() {
    const [trending, setTrending] = useState({})
    const tickers = ['AAPL', 'MSFT', 'AMZN', 'META', 'COIN'];

    React.useEffect(() => {
        api.get('table', {
            method: 'GET'
        }).then(
            res => setTrending(res.data)
        ).catch(
            e => console.log(e)
        )
    }, [])

    return (
        <MDBContainer fluid >
            <SearchBar></SearchBar>
            <MDBRow>
                <MDBCol md='8' style={{ height: '70%' }}>
                    <StockTable trending={trending}></StockTable>
                    <div>
                        <p>Daily most active movers</p>
                        <div className='d-flex justify-content-between'>
                            {tickers.map((item, index) => <TickerBox key={index} name={item}></TickerBox>)}
                        </div>
                    </div>
                </MDBCol>

                <MDBCol md='4' style={{ height: '30%' }}>
                    <p>Current news</p>
                </MDBCol>
            </MDBRow>
        </MDBContainer>
    )
};