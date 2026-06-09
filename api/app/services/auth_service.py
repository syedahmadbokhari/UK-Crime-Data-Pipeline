"""Authentication service — async, SQLAlchemy 2.0 style."""
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.jwt import create_access_token
from app.models.user import User

pwd_context = CryptContext(schemes=["sha256_crypt"], deprecated="auto")


async def register_user(email: str, password: str, db: AsyncSession) -> User:
    result = await db.execute(select(User).where(User.email == email))
    if result.scalar_one_or_none():
        raise ValueError("Email already registered")
    user = User(email=email, hashed_password=pwd_context.hash(password))
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def login_user(email: str, password: str, db: AsyncSession) -> str:
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()
    if not user or not pwd_context.verify(password, user.hashed_password):
        raise ValueError("Invalid credentials")
    return create_access_token({"sub": user.email, "role": user.role})
