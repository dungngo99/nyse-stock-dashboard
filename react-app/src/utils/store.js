import { configureStore } from "@reduxjs/toolkit";
import stockReducer from './redux';

export default configureStore({
    reducer: {
        stock: stockReducer,
    }
});