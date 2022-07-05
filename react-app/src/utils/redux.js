import { createSlice } from '@reduxjs/toolkit'
import api from "./api"

const stockSlice = createSlice({
    'name': 'stock',
    initialState: {
        value: {
            trending: {},
            movers: {},
            news: []
        }
    },
    reducers: {
        addStock: (state, action) => {
            for (const ticker in action.payload) {
                state.value['trending'][ticker] = action['payload'][ticker]
            }
        },
        getMovers: (state, action) => {
            for (const ticker in action.payload) {
                state.value['movers'][ticker] = action['payload'][ticker]
            }
        },
        getNews: (state, action) => {
            for (const i in action.payload) {
                state.value['news'].push(action.payload[i])
            }
        }
    }
})

export const { addStock, getMovers, getNews } = stockSlice.actions

export const addStockAsync = (ticker) => (dispatch) => {
    api.post(`search?ticker=${ticker}`, {
        method: 'POST'
    }).then(
        res => dispatch(addStock(res.data))
    ).catch(
        e => console.log(e)
    )
}

export const getStockAsync = () => (dispatch) => {
    api.get('trending', {
        method: 'GET'
    }).then(
        res => dispatch(addStock(res.data))
    ).catch(
        e => console.log(e)
    )
}

export const getMoversAsync = () => (dispatch) => {
    api.get('movers', {
        method: 'POST'
    }).then(
        res => dispatch(getMovers(res.data))
    ).catch(
        e => console.log(e)
    )
}

export const getNewsAsync = () => (dispatch) => {
    api.get('news', {
        method: 'GET'
    }).then(
        res => dispatch(getNews(res.data.response))
    ).catch(
        e => console.log(e)
    )
}

export const selectStock = (state) => state.stock.value
export default stockSlice.reducer