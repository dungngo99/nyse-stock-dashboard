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
                <MDBCol md='9'>
                    <StockTable trending={trending}></StockTable>
                    <div className='d-flex flex-column'>
                        <p className='align-self-start m-0 p-0' >Daily most active movers</p>
                        <div className='d-flex justify-content-around'>
                            {tickers.map((item, index) => <TickerBox key={index} name={item}></TickerBox>)}
                        </div>
                    </div>
                </MDBCol>

                <MDBCol md='3'>
                    <p>Current news</p>
                </MDBCol>
            </MDBRow>
        </MDBContainer>
    )
};