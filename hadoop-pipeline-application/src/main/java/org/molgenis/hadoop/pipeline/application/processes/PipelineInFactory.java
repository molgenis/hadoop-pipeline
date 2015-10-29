package org.molgenis.hadoop.pipeline.application.processes;

import java.io.InputStream;

/**
 * Factory to retrieve an {@link InStreamHandler} and an {@link InContainer} needed for the handler.
 */
public enum PipelineInFactory
{
	LINES
	{
		/**
		 * @return {@link LinesInContainer}
		 */
		@Override
		public LinesInContainer getContainer()
		{
			return new LinesInContainer();
		}

		/**
		 * @return {@link LinesInStreamHandler}
		 */
		@Override
		public LinesInStreamHandler getInStreamHandler(InputStream inputStream, InContainer inContainer)
				throws ClassCastException
		{
			return new LinesInStreamHandler(inputStream, (LinesInContainer) inContainer);
		}

	},
	SAM
	{
		/**
		 * @return {@link SamInContainer}
		 */
		@Override
		public SamInContainer getContainer()
		{
			return new SamInContainer();
		}

		/**
		 * @return {@link SamInStreamHandler}
		 */
		@Override
		public SamInStreamHandler getInStreamHandler(InputStream inputStream, InContainer inContainer)
				throws ClassCastException
		{
			return new SamInStreamHandler(inputStream, (SamInContainer) inContainer);
		}
	};

	/**
	 * Creates a new {@link InContainer}.
	 * 
	 * @return {@link InContainer}
	 */
	public abstract InContainer getContainer();

	/**
	 * Creates a new {@link InStreamHandler}. If an invalid {@link InContainer} is given, a {@link ClassCastException}
	 * is thrown.
	 * 
	 * @param inputStream
	 * @param inContainer
	 * @return {@link InStreamHandler}
	 * @throws ClassCastException
	 */
	public abstract InStreamHandler getInStreamHandler(InputStream inputStream, InContainer inContainer)
			throws ClassCastException;
}
